/*
 * pg_wait_sampling.c
 *		Track information about wait events.
 *
 * Copyright (c) 2015-2025, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_wait_sampling/pg_wait_sampling.c
 */
#include "postgres.h"

#include <limits.h>

#include "access/htup_details.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "pg_wait_sampling.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#if PG_VERSION_NUM < 150000
#include "postmaster/autovacuum.h"
#include "replication/walsender.h"
#endif

PG_MODULE_MAGIC;

void		_PG_init(void);

static bool shmem_initialized = false;

/* Hooks variables */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static planner_hook_type planner_hook_next = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/* Current nesting depth of planner/Executor calls */
static int	nesting_level = 0;

/* Pointers to shared memory objects */
shm_mq	   *pgws_collector_mq = NULL;
uint64	   *pgws_proc_queryids = NULL;
CollectorShmqHeader *pgws_collector_hdr = NULL;

/* Receiver (backend) local shm_mq pointers and lock */
static shm_mq *recv_mq = NULL;
static shm_mq_handle *recv_mqh = NULL;
static LOCKTAG queueTag;

/* Hook functions */
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static PGPROC *search_proc(int backendPid);
static PlannedStmt *pgws_planner_hook(Query *parse,
#if PG_VERSION_NUM >= 130000
									  const char *query_string,
#endif
									  int cursorOptions, ParamListInfo boundParams);
static
#if PG_VERSION_NUM >= 180000
bool
#else
void
#endif
pgws_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgws_ExecutorRun(QueryDesc *queryDesc,
							 ScanDirection direction,
							 uint64 count
#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 180000
							 ,bool execute_once
#endif
);
static void pgws_ExecutorFinish(QueryDesc *queryDesc);
static void pgws_ExecutorEnd(QueryDesc *queryDesc);
static void pgws_ProcessUtility(PlannedStmt *pstmt,
								const char *queryString,
#if PG_VERSION_NUM >= 140000
								bool readOnlyTree,
#endif
								ProcessUtilityContext context,
								ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest,
#if PG_VERSION_NUM >= 130000
								QueryCompletion *qc
#else
								char *completionTag
#endif
);

/*---- GUC variables ----*/

typedef enum
{
	PGWS_PROFILE_QUERIES_NONE,	/* profile no statements */
	PGWS_PROFILE_QUERIES_TOP,	/* only top level statements */
	PGWS_PROFILE_QUERIES_ALL	/* all statements, including nested ones */
} PGWSTrackLevel;

static const struct config_enum_entry pgws_profile_queries_options[] =
{
	{"none", PGWS_PROFILE_QUERIES_NONE, false},
	{"off", PGWS_PROFILE_QUERIES_NONE, false},
	{"no", PGWS_PROFILE_QUERIES_NONE, false},
	{"false", PGWS_PROFILE_QUERIES_NONE, false},
	{"0", PGWS_PROFILE_QUERIES_NONE, false},
	{"top", PGWS_PROFILE_QUERIES_TOP, false},
	{"on", PGWS_PROFILE_QUERIES_TOP, false},
	{"yes", PGWS_PROFILE_QUERIES_TOP, false},
	{"true", PGWS_PROFILE_QUERIES_TOP, false},
	{"1", PGWS_PROFILE_QUERIES_TOP, false},
	{"all", PGWS_PROFILE_QUERIES_ALL, false},
	{NULL, 0, false}
};

int			pgws_historySize = 5000;
int			pgws_historyPeriod = 10;
int			pgws_profilePeriod = 10;
bool		pgws_profilePid = true;
int			pgws_profileQueries = PGWS_PROFILE_QUERIES_TOP;
bool		pgws_sampleCpu = true;

#define pgws_enabled(level) \
	((pgws_profileQueries == PGWS_PROFILE_QUERIES_ALL) || \
	 (pgws_profileQueries == PGWS_PROFILE_QUERIES_TOP && (level) == 0))

/*
 * Calculate max processes count.
 *
 * The value has to be in sync with ProcGlobal->allProcCount, initialized in
 * InitProcGlobal() (proc.c).
 *
 */
static int
get_max_procs_count(void)
{
	int			count = 0;

	/* First, add the maximum number of backends (MaxBackends). */
#if PG_VERSION_NUM >= 150000

	/*
	 * On pg15+, we can directly access the MaxBackends variable, as it will
	 * have already been initialized in shmem_request_hook.
	 */
	Assert(MaxBackends > 0);
	count += MaxBackends;
#else

	/*
	 * On older versions, we need to compute MaxBackends: bgworkers,
	 * autovacuum workers and launcher. This has to be in sync with the value
	 * computed in InitializeMaxBackends() (postinit.c)
	 *
	 * Note that we need to calculate the value as it won't initialized when
	 * we need it during _PG_init().
	 *
	 * Note also that the value returned during _PG_init() might be different
	 * from the value returned later if some third-party modules change one of
	 * the underlying GUC.  This isn't ideal but can't lead to a crash, as the
	 * value returned during _PG_init() is only used to ask for additional
	 * shmem with RequestAddinShmemSpace(), and postgres has an extra 100kB of
	 * shmem to compensate some small unaccounted usage.  So if the value
	 * later changes, we will allocate and initialize the new (and correct)
	 * memory size, which will either work thanks for the extra 100kB of
	 * shmem, of fail (and prevent postgres startup) due to an out of shared
	 * memory error.
	 */
	count += MaxConnections + autovacuum_max_workers + 1
		+ max_worker_processes;

	/*
	 * Starting with pg12, wal senders aren't part of MaxConnections anymore
	 * and have to be accounted for.
	 */
	count += max_wal_senders;
#endif							/* pg 15- */
	/* End of MaxBackends calculation. */

	/* Add AuxiliaryProcs */
	count += NUM_AUXILIARY_PROCS;

	return count;
}

/*
 * Estimate amount of shared memory needed.
 */
static Size
pgws_shmem_size(void)
{
	shm_toc_estimator e;
	Size		size;
	int			nkeys;

	shm_toc_initialize_estimator(&e);

	nkeys = 3;

	shm_toc_estimate_chunk(&e, sizeof(CollectorShmqHeader));
	shm_toc_estimate_chunk(&e, (Size) COLLECTOR_QUEUE_SIZE);
	shm_toc_estimate_chunk(&e, sizeof(uint64) * get_max_procs_count());

	shm_toc_estimate_keys(&e, nkeys);
	size = shm_toc_estimate(&e);

	return size;
}

#if PG_VERSION_NUM >= 150000
/*
 * shmem_request hook: request additional shared memory resources.
 *
 * If you change code here, don't forget to also report the modifications in
 * _PG_init() for pg14 and below.
 */
static void
pgws_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(pgws_shmem_size());
}
#endif

/*
 * Distribute shared memory.
 */
static void
pgws_shmem_startup(void)
{
	bool		found;
	Size		segsize = pgws_shmem_size();
	void	   *pgws;
	shm_toc    *toc;

	pgws = ShmemInitStruct("pg_wait_sampling", segsize, &found);

	if (!found)
	{
		/* Create shared objects */
		toc = shm_toc_create(PG_WAIT_SAMPLING_MAGIC, pgws, segsize);

		pgws_collector_hdr = shm_toc_allocate(toc, sizeof(CollectorShmqHeader));
		shm_toc_insert(toc, 0, pgws_collector_hdr);
		pgws_collector_mq = shm_toc_allocate(toc, COLLECTOR_QUEUE_SIZE);
		shm_toc_insert(toc, 1, pgws_collector_mq);
		pgws_proc_queryids = shm_toc_allocate(toc,
											  sizeof(uint64) * get_max_procs_count());
		shm_toc_insert(toc, 2, pgws_proc_queryids);
		MemSet(pgws_proc_queryids, 0, sizeof(uint64) * get_max_procs_count());
	}
	else
	{
		/* Attach to existing shared objects */
		toc = shm_toc_attach(PG_WAIT_SAMPLING_MAGIC, pgws);
		pgws_collector_hdr = shm_toc_lookup(toc, 0, false);
		pgws_collector_mq = shm_toc_lookup(toc, 1, false);
		pgws_proc_queryids = shm_toc_lookup(toc, 2, false);
	}

	shmem_initialized = true;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
}

/*
 * Check shared memory is initialized. Report an error otherwise.
 */
static void
check_shmem(void)
{
	if (!shmem_initialized)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("pg_wait_sampling shared memory wasn't initialized yet")));
	}
}

static void
pgws_cleanup_callback(int code, Datum arg)
{
	elog(DEBUG3, "pg_wait_sampling cleanup: detaching shm_mq and releasing queue lock");
	shm_mq_detach(recv_mqh);
	LockRelease(&queueTag, ExclusiveLock, false);
}

/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

#if PG_VERSION_NUM < 150000

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgws_shmem_startup().
	 *
	 * If you change code here, don't forget to also report the modifications
	 * in pgsp_shmem_request() for pg15 and later.
	 */
	RequestAddinShmemSpace(pgws_shmem_size());
#endif

	pgws_register_wait_collector();

	/*
	 * Install hooks.
	 */
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pgws_shmem_request;
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgws_shmem_startup;
	planner_hook_next = planner_hook;
	planner_hook = pgws_planner_hook;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgws_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgws_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgws_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgws_ExecutorEnd;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pgws_ProcessUtility;

	/* Define GUC variables */
	DefineCustomIntVariable("pg_wait_sampling.history_size",
							"Sets size of waits history.",
							NULL,
							&pgws_historySize,
							5000,
							100,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_wait_sampling.history_period",
							"Sets period of waits history sampling.",
							NULL,
							&pgws_historyPeriod,
							10,
							1,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_wait_sampling.profile_period",
							"Sets period of waits profile sampling.",
							NULL,
							&pgws_profilePeriod,
							10,
							1,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_wait_sampling.profile_pid",
							 "Sets whether profile should be collected per pid.",
							 NULL,
							 &pgws_profilePid,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("pg_wait_sampling.profile_queries",
							 "Sets whether profile should be collected per query.",
							 NULL,
							 &pgws_profileQueries,
							 PGWS_PROFILE_QUERIES_TOP,
							 pgws_profile_queries_options,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_wait_sampling.sample_cpu",
							 "Sets whether not waiting backends should be sampled.",
							 NULL,
							 &pgws_sampleCpu,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

#if PG_VERSION_NUM >= 150000
	MarkGUCPrefixReserved("pg_wait_sampling");
#endif
}

/*
 * Find PGPROC entry responsible for given pid assuming ProcArrayLock was
 * already taken.
 */
static PGPROC *
search_proc(int pid)
{
	int			i;

	if (pid == 0)
		return MyProc;

	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		PGPROC	   *proc = &ProcGlobal->allProcs[i];

		if (proc->pid && proc->pid == pid)
		{
			return proc;
		}
	}

	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("backend with pid=%d not found", pid)));
	return NULL;
}

/*
 * Decide whether this PGPROC entry should be included in profiles and output
 * views.
 */
bool
pgws_should_sample_proc(PGPROC *proc, int *pid_p, uint32 *wait_event_info_p)
{
	int			pid = proc->pid;
	uint32		wait_event_info = proc->wait_event_info;

	*pid_p = pid;
	*wait_event_info_p = wait_event_info;

	if (wait_event_info == 0 && !pgws_sampleCpu)
		return false;

	/*
	 * On PostgreSQL versions < 17 the PGPROC->pid field is not reset on
	 * process exit. This would lead to such processes getting counted for
	 * null wait events. So instead we make use of DisownLatch() resetting
	 * owner_pid during ProcKill().
	 */
	if (pid == 0 || proc->procLatch.owner_pid == 0 || pid == MyProcPid)
		return false;

	return true;
}

typedef struct
{
	HistoryItem *items;
	TimestampTz ts;
} WaitCurrentContext;

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_current);
Datum
pg_wait_sampling_get_current(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	WaitCurrentContext *params;

	check_shmem();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		params = (WaitCurrentContext *) palloc0(sizeof(WaitCurrentContext));
		params->ts = GetCurrentTimestamp();

		funcctx->user_fctx = params;
		tupdesc = CreateTemplateTupleDesc(4);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "event",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "queryid",
						   INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		LWLockAcquire(ProcArrayLock, LW_SHARED);

		if (!PG_ARGISNULL(0))
		{
			/* pg_wait_sampling_get_current(pid int4) function */
			HistoryItem *item;
			PGPROC	   *proc;

			proc = search_proc(PG_GETARG_UINT32(0));
			params->items = (HistoryItem *) palloc0(sizeof(HistoryItem));
			item = &params->items[0];
			item->pid = proc->pid;
			item->wait_event_info = proc->wait_event_info;
			item->queryId = pgws_proc_queryids[proc - ProcGlobal->allProcs];
			funcctx->max_calls = 1;
		}
		else
		{
			/* pg_wait_sampling_current view */
			int			procCount = ProcGlobal->allProcCount,
						i,
						j = 0;

			params->items = (HistoryItem *) palloc0(sizeof(HistoryItem) * procCount);
			for (i = 0; i < procCount; i++)
			{
				PGPROC	   *proc = &ProcGlobal->allProcs[i];

				if (!pgws_should_sample_proc(proc,
											 &params->items[j].pid,
											 &params->items[j].wait_event_info))
					continue;

				params->items[j].pid = proc->pid;
				params->items[j].wait_event_info = proc->wait_event_info;
				params->items[j].queryId = pgws_proc_queryids[i];
				j++;
			}
			funcctx->max_calls = j;
		}

		LWLockRelease(ProcArrayLock);

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	params = (WaitCurrentContext *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		HeapTuple	tuple;
		Datum		values[4];
		bool		nulls[4];
		const char *event_type,
				   *event;
		HistoryItem *item;

		item = &params->items[funcctx->call_cntr];

		/* Make and return next tuple to caller */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		event_type = pgstat_get_wait_event_type(item->wait_event_info);
		event = pgstat_get_wait_event(item->wait_event_info);
		values[0] = Int32GetDatum(item->pid);
		if (event_type)
			values[1] = PointerGetDatum(cstring_to_text(event_type));
		else
			nulls[1] = true;
		if (event)
			values[2] = PointerGetDatum(cstring_to_text(event));
		else
			nulls[2] = true;

		values[3] = UInt64GetDatum(item->queryId);
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}
}

typedef struct
{
	Size		count;
	ProfileItem *items;
} Profile;

void
pgws_init_lock_tag(LOCKTAG *tag, uint32 lock)
{
	tag->locktag_field1 = PG_WAIT_SAMPLING_MAGIC;
	tag->locktag_field2 = lock;
	tag->locktag_field3 = 0;
	tag->locktag_field4 = 0;
	tag->locktag_type = LOCKTAG_USERLOCK;
	tag->locktag_lockmethodid = USER_LOCKMETHOD;
}

/* Get array (history or profile data) from shared memory */
static void *
receive_array(SHMRequest request, Size item_size, Size *count)
{
	LOCKTAG		collectorTag;
	shm_mq_result res;
	Size		len,
				i;
	void	   *data;
	Pointer		result,
				ptr;
	MemoryContext oldctx;

	/* Ensure nobody else trying to send request to queue */
	pgws_init_lock_tag(&queueTag, PGWS_QUEUE_LOCK);
	LockAcquire(&queueTag, ExclusiveLock, false, false);

	pgws_init_lock_tag(&collectorTag, PGWS_COLLECTOR_LOCK);
	LockAcquire(&collectorTag, ExclusiveLock, false, false);
	recv_mq = shm_mq_create(pgws_collector_mq, COLLECTOR_QUEUE_SIZE);
	pgws_collector_hdr->request = request;
	LockRelease(&collectorTag, ExclusiveLock, false);

	if (!pgws_collector_hdr->latch)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("pg_wait_sampling collector wasn't started")));

	SetLatch(pgws_collector_hdr->latch);

	shm_mq_set_receiver(recv_mq, MyProc);

	/*
	 * We switch to TopMemoryContext, so that recv_mqh is allocated there and
	 * is guaranteed to survive until before_shmem_exit callbacks are fired.
	 * Anyway, shm_mq_detach() will free handler on its own.
	 *
	 * NB: we do not pass `seg` to shm_mq_attach(), so it won't set its own
	 * callback, i.e. we do not interfere here with shm_mq_detach_callback().
	 */
	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	recv_mqh = shm_mq_attach(recv_mq, NULL, NULL);
	MemoryContextSwitchTo(oldctx);

	/*
	 * Now we surely attached to the shm_mq and got collector's attention. If
	 * anything went wrong (e.g. Ctrl+C received from the client) we have to
	 * cleanup some things, i.e. detach from the shm_mq, so collector was able
	 * to continue responding to other requests.
	 *
	 * PG_ENSURE_ERROR_CLEANUP() guaranties that cleanup callback will be
	 * fired for both ERROR and FATAL.
	 */
	PG_ENSURE_ERROR_CLEANUP(pgws_cleanup_callback, 0);
	{
		res = shm_mq_receive(recv_mqh, &len, &data, false);
		if (res != SHM_MQ_SUCCESS || len != sizeof(*count))
			elog(ERROR, "error reading mq");

		memcpy(count, data, sizeof(*count));

		result = palloc(item_size * (*count));
		ptr = result;

		for (i = 0; i < *count; i++)
		{
			res = shm_mq_receive(recv_mqh, &len, &data, false);
			if (res != SHM_MQ_SUCCESS || len != item_size)
				elog(ERROR, "error reading mq");

			memcpy(ptr, data, item_size);
			ptr += item_size;
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(pgws_cleanup_callback, 0);

	/* We still have to detach and release lock during normal operation. */
	shm_mq_detach(recv_mqh);
	LockRelease(&queueTag, ExclusiveLock, false);

	return result;
}


PG_FUNCTION_INFO_V1(pg_wait_sampling_get_profile);
Datum
pg_wait_sampling_get_profile(PG_FUNCTION_ARGS)
{
	Profile    *profile;
	FuncCallContext *funcctx;

	check_shmem();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Receive profile from shmq */
		profile = (Profile *) palloc0(sizeof(Profile));
		profile->items = (ProfileItem *) receive_array(PROFILE_REQUEST,
													   sizeof(ProfileItem), &profile->count);

		funcctx->user_fctx = profile;
		funcctx->max_calls = profile->count;

		/* Make tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(5);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "event",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "queryid",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "count",
						   INT8OID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	profile = (Profile *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		/* for each row */
		Datum		values[5];
		bool		nulls[5];
		HeapTuple	tuple;
		ProfileItem *item;
		const char *event_type,
				   *event;

		item = &profile->items[funcctx->call_cntr];

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		/* Make and return next tuple to caller */
		event_type = pgstat_get_wait_event_type(item->wait_event_info);
		event = pgstat_get_wait_event(item->wait_event_info);
		values[0] = Int32GetDatum(item->pid);
		if (event_type)
			values[1] = PointerGetDatum(cstring_to_text(event_type));
		else
			nulls[1] = true;
		if (event)
			values[2] = PointerGetDatum(cstring_to_text(event));
		else
			nulls[2] = true;

		if (pgws_profileQueries)
			values[3] = UInt64GetDatum(item->queryId);
		else
			values[3] = (Datum) 0;

		values[4] = UInt64GetDatum(item->count);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}
}

PG_FUNCTION_INFO_V1(pg_wait_sampling_reset_profile);
Datum
pg_wait_sampling_reset_profile(PG_FUNCTION_ARGS)
{
	LOCKTAG		collectorTag;

	check_shmem();

	pgws_init_lock_tag(&queueTag, PGWS_QUEUE_LOCK);

	LockAcquire(&queueTag, ExclusiveLock, false, false);

	pgws_init_lock_tag(&collectorTag, PGWS_COLLECTOR_LOCK);
	LockAcquire(&collectorTag, ExclusiveLock, false, false);
	pgws_collector_hdr->request = PROFILE_RESET;
	LockRelease(&collectorTag, ExclusiveLock, false);

	SetLatch(pgws_collector_hdr->latch);

	LockRelease(&queueTag, ExclusiveLock, false);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_history);
Datum
pg_wait_sampling_get_history(PG_FUNCTION_ARGS)
{
	History    *history;
	FuncCallContext *funcctx;

	check_shmem();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Receive history from shmq */
		history = (History *) palloc0(sizeof(History));
		history->items = (HistoryItem *) receive_array(HISTORY_REQUEST,
													   sizeof(HistoryItem), &history->count);

		funcctx->user_fctx = history;
		funcctx->max_calls = history->count;

		/* Make tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(5);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "sample_ts",
						   TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "event",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "queryid",
						   INT8OID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	history = (History *) funcctx->user_fctx;

	if (history->index < history->count)
	{
		HeapTuple	tuple;
		HistoryItem *item;
		Datum		values[5];
		bool		nulls[5];
		const char *event_type,
				   *event;

		item = &history->items[history->index];

		/* Make and return next tuple to caller */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		event_type = pgstat_get_wait_event_type(item->wait_event_info);
		event = pgstat_get_wait_event(item->wait_event_info);
		values[0] = Int32GetDatum(item->pid);
		values[1] = TimestampTzGetDatum(item->ts);
		if (event_type)
			values[2] = PointerGetDatum(cstring_to_text(event_type));
		else
			nulls[2] = true;
		if (event)
			values[3] = PointerGetDatum(cstring_to_text(event));
		else
			nulls[3] = true;

		values[4] = UInt64GetDatum(item->queryId);
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		history->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}

	PG_RETURN_VOID();
}

/*
 * planner_hook hook, save queryId for collector
 */
static PlannedStmt *
pgws_planner_hook(Query *parse,
#if PG_VERSION_NUM >= 130000
				  const char *query_string,
#endif
				  int cursorOptions,
				  ParamListInfo boundParams)
{
	PlannedStmt *result;
	int			i = MyProc - ProcGlobal->allProcs;
	uint64		save_queryId = 0;

	if (pgws_enabled(nesting_level))
	{
		save_queryId = pgws_proc_queryids[i];
		pgws_proc_queryids[i] = parse->queryId;
	}

	nesting_level++;
	PG_TRY();
	{
		/* Invoke original hook if needed */
		if (planner_hook_next)
			result = planner_hook_next(parse,
#if PG_VERSION_NUM >= 130000
									   query_string,
#endif
									   cursorOptions, boundParams);
		else
			result = standard_planner(parse,
#if PG_VERSION_NUM >= 130000
									  query_string,
#endif
									  cursorOptions, boundParams);
		nesting_level--;
		if (nesting_level == 0)
			pgws_proc_queryids[i] = UINT64CONST(0);
		else if (pgws_enabled(nesting_level))
			pgws_proc_queryids[i] = save_queryId;
	}
	PG_CATCH();
	{
		nesting_level--;
		if (nesting_level == 0)
			pgws_proc_queryids[i] = UINT64CONST(0);
		else if (pgws_enabled(nesting_level))
			pgws_proc_queryids[i] = save_queryId;
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

/*
 * ExecutorStart hook: save queryId for collector
 */
static
#if PG_VERSION_NUM >= 180000
bool
#else
void
#endif
pgws_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	int			i = MyProc - ProcGlobal->allProcs;

	if (pgws_enabled(nesting_level))
		pgws_proc_queryids[i] = queryDesc->plannedstmt->queryId;
	if (prev_ExecutorStart)
		return prev_ExecutorStart(queryDesc, eflags);
	else
		return standard_ExecutorStart(queryDesc, eflags);
}

static void
pgws_ExecutorRun(QueryDesc *queryDesc,
				 ScanDirection direction,
				 uint64 count
#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 180000
				 ,bool execute_once
#endif
)
{
	int			i = MyProc - ProcGlobal->allProcs;
	uint64		save_queryId = pgws_proc_queryids[i];

	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 180000
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			prev_ExecutorRun(queryDesc, direction, count);
#endif
		else
#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 180000
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			standard_ExecutorRun(queryDesc, direction, count);
#endif
		nesting_level--;
		if (nesting_level == 0)
			pgws_proc_queryids[i] = UINT64CONST(0);
		else
			pgws_proc_queryids[i] = save_queryId;
	}
	PG_CATCH();
	{
		nesting_level--;
		if (nesting_level == 0)
			pgws_proc_queryids[i] = UINT64CONST(0);
		else
			pgws_proc_queryids[i] = save_queryId;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
pgws_ExecutorFinish(QueryDesc *queryDesc)
{
	int			i = MyProc - ProcGlobal->allProcs;
	uint64		save_queryId = pgws_proc_queryids[i];

	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nesting_level--;
		if (nesting_level == 0)
			pgws_proc_queryids[i] = UINT64CONST(0);
		else
			pgws_proc_queryids[i] = save_queryId;
	}
	PG_CATCH();
	{
		nesting_level--;
		pgws_proc_queryids[i] = save_queryId;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorEnd hook: clear queryId
 */
static void
pgws_ExecutorEnd(QueryDesc *queryDesc)
{
	int			i = MyProc - ProcGlobal->allProcs;

	if (nesting_level == 0)
		pgws_proc_queryids[i] = UINT64CONST(0);

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

static void
pgws_ProcessUtility(PlannedStmt *pstmt,
					const char *queryString,
#if PG_VERSION_NUM >= 140000
					bool readOnlyTree,
#endif
					ProcessUtilityContext context,
					ParamListInfo params,
					QueryEnvironment *queryEnv,
					DestReceiver *dest,
#if PG_VERSION_NUM >= 130000
					QueryCompletion *qc
#else
					char *completionTag
#endif
)
{
	int			i = MyProc - ProcGlobal->allProcs;
	uint64		save_queryId = 0;

	if (pgws_enabled(nesting_level))
	{
		save_queryId = pgws_proc_queryids[i];
		pgws_proc_queryids[i] = pstmt->queryId;
	}

	nesting_level++;
	PG_TRY();
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
								readOnlyTree,
#endif
								context, params, queryEnv,
								dest,
#if PG_VERSION_NUM >= 130000
								qc
#else
								completionTag
#endif
				);
		else
			standard_ProcessUtility(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
									readOnlyTree,
#endif
									context, params, queryEnv,
									dest,
#if PG_VERSION_NUM >= 130000
									qc
#else
									completionTag
#endif
				);
		nesting_level--;
		if (nesting_level == 0)
			pgws_proc_queryids[i] = UINT64CONST(0);
		else if (pgws_enabled(nesting_level))
			pgws_proc_queryids[i] = save_queryId;
	}
	PG_CATCH();
	{
		nesting_level--;
		if (nesting_level == 0)
			pgws_proc_queryids[i] = UINT64CONST(0);
		else if (pgws_enabled(nesting_level))
			pgws_proc_queryids[i] = save_queryId;
		PG_RE_THROW();
	}
	PG_END_TRY();
}
