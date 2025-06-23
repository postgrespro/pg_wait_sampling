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
#include "common/ip.h"
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
#include "utils/varlena.h"

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
static void pgws_ExecutorStart(QueryDesc *queryDesc, int eflags);
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
static char *pgws_history_dimensions_string = NULL;
static char *pgws_profile_dimensions_string = NULL;
int			pgws_history_dimensions; /* bit mask that is derived from GUC */
int			pgws_profile_dimensions; /* bit mask that is derived from GUC */

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
 * Check tokens of string and fill bitmask accordingly
 * Mostly copied from plpgsql_extra_checks_check_hook
 */
static bool
pgws_general_dimensions_check_hook (char **newvalue, void **extra, GucSource source)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *l;
	int			extrachecks = 0;
	int		   *myextra;

	/* Check special cases when we turn all or none dimensions */
	if (pg_strcasecmp(*newvalue, "all") == 0)
		extrachecks = PGWS_DIMENSIONS_ALL;
	else if (pg_strcasecmp(*newvalue, "none") == 0)
		extrachecks = PGWS_DIMENSIONS_NONE;
	else
	{
		/* Need a modifiable copy of string */
		rawstring = pstrdup(*newvalue);

		/* Parse string into list of identifiers */
		if (!SplitIdentifierString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			GUC_check_errdetail("List syntax is invalid.");
			pfree(rawstring);
			list_free(elemlist);
			return false;
		}

		/* Loop over all recieved options */
		foreach(l, elemlist)
		{
			char	   *tok = (char *) lfirst(l);

			/* Process all allowed values */
			if (pg_strcasecmp(tok, "pid") == 0)
				extrachecks |= PGWS_DIMENSIONS_PID;
			else if (pg_strcasecmp(tok, "wait_event_type") == 0)
				extrachecks |= PGWS_DIMENSIONS_WAIT_EVENT_TYPE;
			else if (pg_strcasecmp(tok, "wait_event") == 0)
				extrachecks |= PGWS_DIMENSIONS_WAIT_EVENT;
			else if (pg_strcasecmp(tok, "query_id") == 0)
				extrachecks |= PGWD_DIMENSIONS_QUERY_ID;
			else if (pg_strcasecmp(tok, "role_id") == 0)
				extrachecks |= PGWS_DIMENSIONS_ROLE_ID;
			else if (pg_strcasecmp(tok, "database_id") == 0)
				extrachecks |= PGWS_DIMENSIONS_DB_ID;
			else if (pg_strcasecmp(tok, "parallel_leader_pid") == 0)
				extrachecks |= PGWS_DIMENSIONS_PARALLEL_LEADER_PID;
			else if (pg_strcasecmp(tok, "is_regular_backend") == 0)
				extrachecks |= PGWS_DIMENSIONS_IS_REGULAR_BE;
			else if (pg_strcasecmp(tok, "backend_type") == 0)
				extrachecks |= PGWS_DIMENSIONS_BE_TYPE;
			else if (pg_strcasecmp(tok, "backend_state") == 0)
				extrachecks |= PGWS_DIMENSIONS_BE_STATE;
			else if (pg_strcasecmp(tok, "backend_start_time") == 0)
				extrachecks |= PGWS_DIMENSIONS_BE_START_TIME;
			else if (pg_strcasecmp(tok, "client_addr") == 0)
				extrachecks |= PGWS_DIMENSIONS_CLIENT_ADDR;
			else if (pg_strcasecmp(tok, "client_hostname") == 0)
				extrachecks |= PGWS_DIMENSIONS_CLIENT_HOSTNAME;
			else if (pg_strcasecmp(tok, "appname") == 0)
				extrachecks |= PGWS_DIMENSIONS_APPNAME;
			else if (pg_strcasecmp(tok, "all") == 0 || pg_strcasecmp(tok, "none") == 0)
			{
				GUC_check_errdetail("Key word \"%s\" cannot be combined with other key words.", tok);
				pfree(rawstring);
				list_free(elemlist);
				return false;
			}
			else
			{
				GUC_check_errdetail("Unrecognized key word: \"%s\".", tok);
				pfree(rawstring);
				list_free(elemlist);
				return false;
			}
		}

		pfree(rawstring);
		list_free(elemlist);
	}
#if PG_VERSION_NUM >= 160000
	myextra = (int *) guc_malloc(LOG, sizeof(int));
#else
	myextra = (int *) malloc(sizeof(int));
#endif
	if (!myextra)
		return false;
	*myextra = extrachecks;
	*extra = myextra;

	return true;
}

/* Assign actual value to dimension bitmask */
static void
pgws_history_dimensions_assign_hook (const char *newvalue, void *extra)
{
	pgws_history_dimensions = *((int *) extra);
}

/* Assign actual value to dimension bitmask */
static void
pgws_profile_dimensions_assign_hook (const char *newvalue, void *extra)
{
	pgws_profile_dimensions = *((int *) extra);
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
							0,
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
							0,
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

	DefineCustomStringVariable("pg_wait_sampling.history_dimensions",
							   "Sets sampling dimensions for history",
							   NULL,
							   &pgws_history_dimensions_string,
							   "pid, wait_event_type, wait_event, query_id",
							   PGC_SIGHUP,
							   GUC_LIST_INPUT,
							   pgws_general_dimensions_check_hook,
							   pgws_history_dimensions_assign_hook,
							   NULL);

	DefineCustomStringVariable("pg_wait_sampling.profile_dimensions",
							   "Sets sampling dimensions for profile",
							   NULL,
							   &pgws_profile_dimensions_string,
							   "pid, wait_event_type, wait_event, query_id",
							   PGC_SIGHUP,
							   GUC_LIST_INPUT,
							   pgws_general_dimensions_check_hook,
							   pgws_profile_dimensions_assign_hook,
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

//TODO OBSOLETE
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
			item->dimensions.pid = proc->pid;
			item->dimensions.wait_event_info = proc->wait_event_info;
			item->dimensions.queryId = pgws_proc_queryids[proc - ProcGlobal->allProcs];
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
											 &params->items[j].dimensions.pid,
											 &params->items[j].dimensions.wait_event_info))
					continue;

				params->items[j].dimensions.pid = proc->pid;
				params->items[j].dimensions.wait_event_info = proc->wait_event_info;
				params->items[j].dimensions.queryId = pgws_proc_queryids[i];
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

		event_type = pgstat_get_wait_event_type(item->dimensions.wait_event_info);
		event = pgstat_get_wait_event(item->dimensions.wait_event_info);
		values[0] = Int32GetDatum(item->dimensions.pid);
		if (event_type)
			values[1] = PointerGetDatum(cstring_to_text(event_type));
		else
			nulls[1] = true;
		if (event)
			values[2] = PointerGetDatum(cstring_to_text(event));
		else
			nulls[2] = true;

		values[3] = UInt64GetDatum(item->dimensions.queryId);
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}
}

static Datum
GetBackendState(BackendState state, bool *is_null)
{
	switch (state)
	{
#if PG_VERSION_NUM >= 180000
		case STATE_STARTING:
			return CStringGetTextDatum("starting");
#endif
		case STATE_IDLE:
			return CStringGetTextDatum("idle");
		case STATE_RUNNING:
			return CStringGetTextDatum("active");
		case STATE_IDLEINTRANSACTION:
			return CStringGetTextDatum("idle in transaction");
		case STATE_FASTPATH:
			return CStringGetTextDatum("fastpath function call");
		case STATE_IDLEINTRANSACTION_ABORTED:
			return CStringGetTextDatum("idle in transaction (aborted)");
		case STATE_DISABLED:
			return CStringGetTextDatum("disabled");
		case STATE_UNDEFINED:
			*is_null = true;
	}
	return (Datum) 0;
}

/* Copied from pg_stat_get_backend_client_addr */
static Datum
get_backend_client_addr(SockAddr client_addr, bool *is_null)
{
	char		remote_host[NI_MAXHOST];
	int			ret;

	/* A zeroed client addr means we don't know */
#if PG_VERSION_NUM >= 180000
	if (pg_memory_is_all_zeros(&client_addr,
							   sizeof(client_addr)))
#else
	SockAddr	zero_clientaddr;

	memset(&zero_clientaddr, 0, sizeof(zero_clientaddr));
	if (memcmp(&client_addr, &zero_clientaddr,
			   sizeof(zero_clientaddr)) == 0)
#endif
	{
		*is_null = true;
		return (Datum) 0;
	}

	switch (client_addr.addr.ss_family)
	{
		case AF_INET:
		case AF_INET6:
			break;
		default:
			*is_null = true;
			return (Datum) 0;
	}

	remote_host[0] = '\0';
	ret = pg_getnameinfo_all(&client_addr.addr,
							 client_addr.salen,
							 remote_host, sizeof(remote_host),
							 NULL, 0,
							 NI_NUMERICHOST | NI_NUMERICSERV);
	if (ret != 0)
	{
		*is_null = true;
		return (Datum) 0;
	}

	clean_ipv6_addr(client_addr.addr.ss_family, remote_host);

	return (DirectFunctionCall1(inet_in, CStringGetDatum(remote_host)));
}

/*
 * Needed for PostgreSQL 16 and earlier since there is no good way to get
 * PgBackendStatus when having only PGPROC structure.
 *
 * pgstat_fetch_stat_beentry (13-15) works with indices of localBackendStatusTable
 * pgstat_get_beentry_by_backend_id (16) works with "backend_ids", but we still
 * cannot get them without looking into LocalPgBackendStatus, so work with indices
 *
 * This function is very inefficient
 *
 * Maybe we should just iterate over localBackendStatusTable and somehow get
 * PGPROC entries from there but it is up for discussion
 */
PgBackendStatus *
get_beentry_by_procpid(int pid)
{
	int backend_num = pgstat_fetch_stat_numbackends(), cur_be_idx;

	for (cur_be_idx = 1; cur_be_idx <= backend_num; cur_be_idx++)
	{
		LocalPgBackendStatus *local_beentry;

#if PG_VERSION_NUM >= 160000
		local_beentry = pgstat_get_local_beentry_by_index(cur_be_idx);
#else
		/* Here beid is just index in localBackendStatusTable */
		local_beentry = pgstat_fetch_stat_local_beentry(cur_be_idx);
#endif
		if (local_beentry->backendStatus->st_procpid == pid)
			return local_beentry->backendStatus;
	}
	return NULL;
}

/*
 * Common routine to fill "dimensions" part of tupdesc
 */
static void
fill_tuple_desc (TupleDesc tupdesc)
{
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "type",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "event",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "queryid",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "role_id",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "database_id",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7, "parallel_leader_pid",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 8, "is_regular_backend",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 9, "backend_type",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 10, "backend_state",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 11, "proc_start",
					   TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 12, "client_addr",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 13, "client_hostname",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 14, "appname",
					   TEXTOID, -1, 0);
}

static void
fill_values_and_nulls(Datum *values, bool *nulls, SamplingDimensions dimensions, int dimensions_mask)
{
	const char *event_type,
			   *event,
			   *backend_type;
	Datum		backend_state, proc_start, client_addr;
	bool		is_null_be_state = false,
				is_null_client_addr = false;

	event_type = pgstat_get_wait_event_type(dimensions.wait_event_info);
	event = pgstat_get_wait_event(dimensions.wait_event_info);
	backend_type = GetBackendTypeDesc(dimensions.backend_type);
	backend_state = GetBackendState(dimensions.backend_state, &is_null_be_state);
	proc_start = TimestampTzGetDatum(dimensions.proc_start);
	client_addr = get_backend_client_addr(dimensions.client_addr, &is_null_client_addr);

	if (dimensions_mask & PGWS_DIMENSIONS_PID)
		values[0] = Int32GetDatum(dimensions.pid);
	else
		values[0] = (Datum) 0;
	if (event_type && (dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT_TYPE))
		values[1] = PointerGetDatum(cstring_to_text(event_type));
	else
		nulls[1] = true;
	if (event && (dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT))
		values[2] = PointerGetDatum(cstring_to_text(event));
	else
		nulls[2] = true;
	if (pgws_profileQueries || (dimensions_mask & PGWD_DIMENSIONS_QUERY_ID))
		values[3] = UInt64GetDatum(dimensions.queryId);
	else
		values[3] = (Datum) 0;
	if (dimensions_mask & PGWS_DIMENSIONS_ROLE_ID)
		values[4] = ObjectIdGetDatum(dimensions.role_id);
	else
		nulls[4] = true;
	if (dimensions_mask & PGWS_DIMENSIONS_DB_ID)
		values[5] = ObjectIdGetDatum(dimensions.database_id);
	else
		nulls[5] = true;
	if (dimensions_mask & PGWS_DIMENSIONS_PARALLEL_LEADER_PID)
		values[6] = Int32GetDatum(dimensions.parallel_leader_pid);
	else
		nulls[6] = true;
	if (dimensions_mask & PGWS_DIMENSIONS_IS_REGULAR_BE)
		values[7] = BoolGetDatum(dimensions.is_regular_backend);
	else
		nulls[7] = true;
	if (backend_type && (dimensions_mask & PGWS_DIMENSIONS_BE_TYPE))
		values[8] = PointerGetDatum(cstring_to_text(backend_type));
	else
		nulls[8] = true;
	if (!is_null_be_state && (dimensions_mask & PGWS_DIMENSIONS_BE_STATE))
		values[9] = backend_state;
	else
		nulls[9] = true;
	if (dimensions_mask & PGWS_DIMENSIONS_BE_START_TIME)
		values[10] = proc_start;
	else
		nulls[10] = true;
	if (!is_null_client_addr && (dimensions_mask & PGWS_DIMENSIONS_CLIENT_ADDR))
		values[11] = client_addr;
	else
		nulls[11] = true;
	if (dimensions_mask & PGWS_DIMENSIONS_CLIENT_HOSTNAME)
		values[12] = PointerGetDatum(cstring_to_text(dimensions.client_hostname));
	else
		nulls[12] = true;
	if (dimensions_mask & PGWS_DIMENSIONS_APPNAME)
		values[13] = PointerGetDatum(cstring_to_text(dimensions.appname));
	else
		nulls[13] = true;
}

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_current_extended);
Datum
pg_wait_sampling_get_current_extended(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	WaitCurrentContext *params;

	check_shmem();

	/* Initialization, done only on the first call */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		params = (WaitCurrentContext *) palloc0(sizeof(WaitCurrentContext));
		params->ts = GetCurrentTimestamp();

		funcctx->user_fctx = params;
		/* Setup tuple desc */
		tupdesc = CreateTemplateTupleDesc(14);
		fill_tuple_desc (tupdesc);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		LWLockAcquire(ProcArrayLock, LW_SHARED);

		if (!PG_ARGISNULL(0))
		{
			/* pg_wait_sampling_get_current_extended(pid int4) function */
			HistoryItem		*item;
			PGPROC			*proc;
			//PgBackendStatus *bestatus; not needed?

			proc = search_proc(PG_GETARG_UINT32(0));

			params->items = (HistoryItem *) palloc0(sizeof(HistoryItem));
			item = &params->items[0];

			fill_dimensions(&item->dimensions, proc, proc->pid,
							proc->wait_event_info,
							pgws_proc_queryids[proc - ProcGlobal->allProcs],
							PGWS_DIMENSIONS_ALL);

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
				PGPROC 			*proc = &ProcGlobal->allProcs[i];

				if (!pgws_should_sample_proc(proc,
											 &params->items[j].dimensions.pid,
											 &params->items[j].dimensions.wait_event_info))
					continue;

				fill_dimensions(&params->items[j].dimensions, proc, proc->pid,
								proc->wait_event_info,
								pgws_proc_queryids[proc - ProcGlobal->allProcs],
								PGWS_DIMENSIONS_ALL);

				j++;
			}
			funcctx->max_calls = j;
		}

		LWLockRelease(ProcArrayLock);
#if PG_VERSION_NUM >= 140000
		pgstat_clear_backend_activity_snapshot();
#else
		pgstat_clear_snapshot();
#endif

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	params = (WaitCurrentContext *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		HeapTuple	tuple;
		Datum		values[14];
		bool		nulls[14];
		HistoryItem *item;

		item = &params->items[funcctx->call_cntr];

		/* Make and return next tuple to caller */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		fill_values_and_nulls(values, nulls, item->dimensions, PGWS_DIMENSIONS_ALL);

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
receive_array(SHMRequest request, Size *item_size, Size *count, int *dimensions_mask)
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

        /*
         * Check that the collector was started to avoid NULL
         * pointer dereference.
         */
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

		res = shm_mq_receive(recv_mqh, &len, &data, false);
		if (res != SHM_MQ_SUCCESS || len != sizeof(*dimensions_mask))
			elog(ERROR, "error reading mq");

		memcpy(dimensions_mask, data, sizeof(*dimensions_mask));

		*item_size = get_serialized_size(*dimensions_mask, true);

		result = palloc(*item_size * (*count));
		ptr = result;

		for (i = 0; i < *count; i++)
		{
			res = shm_mq_receive(recv_mqh, &len, &data, false);
			if (res != SHM_MQ_SUCCESS || len != *item_size)
				elog(ERROR, "error reading mq");

			memcpy(ptr, data, *item_size);
			ptr += *item_size;
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(pgws_cleanup_callback, 0);

	/* We still have to detach and release lock during normal operation. */
	shm_mq_detach(recv_mqh);
	LockRelease(&queueTag, ExclusiveLock, false);

	return result;
}

static void *
deserialize_array(void *tmp_array, int count, bool is_history)
{
	Pointer result,
			ptr;
	int 	i;
	int		dimensions_mask = (is_history ? saved_history_dimensions : saved_profile_dimensions);
	int		serialized_size = get_serialized_size(dimensions_mask, true);

	result = palloc0((is_history ? sizeof(HistoryItem) : sizeof(ProfileItem)) * count);
	ptr = result;

	for (i = 0; i < count; i++)
	{
		SamplingDimensions	tmp_dimensions;
		char			   *cur_item;
		TimestampTz		   *ts;
		uint64			   *count;
		int					ts_count_size = sizeof(uint64); /* is 8 bytes anyway */

		cur_item = (((char *) tmp_array) + i * serialized_size);
		ts = (is_history ? palloc0(sizeof(TimestampTz)) : NULL);
		count = (is_history ? NULL : palloc0(sizeof(uint64)));

		deserialize_item(&tmp_dimensions, cur_item, dimensions_mask, ts, count);

		memcpy(ptr, &tmp_dimensions, sizeof(SamplingDimensions));
		ptr += sizeof(SamplingDimensions);
		if (is_history)
		{
			memcpy(ptr, ts, ts_count_size);
			ptr += sizeof(TimestampTz);
		}
		else
		{
			memcpy(ptr, count, ts_count_size);
			ptr += sizeof(uint64);
		}
	}

	return result;
}

//TODO OBSOLETE
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
		//profile->items = (ProfileItem *) receive_array(PROFILE_REQUEST,
		//											   sizeof(ProfileItem), &profile->count);

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
		event_type = pgstat_get_wait_event_type(item->dimensions.wait_event_info);
		event = pgstat_get_wait_event(item->dimensions.wait_event_info);
		values[0] = Int32GetDatum(item->dimensions.pid);
		if (event_type)
			values[1] = PointerGetDatum(cstring_to_text(event_type));
		else
			nulls[1] = true;
		if (event)
			values[2] = PointerGetDatum(cstring_to_text(event));
		else
			nulls[2] = true;

		if (pgws_profileQueries)
			values[3] = UInt64GetDatum(item->dimensions.queryId);
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

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_profile_extended);
Datum
pg_wait_sampling_get_profile_extended(PG_FUNCTION_ARGS)
{
	Profile    *profile;
	FuncCallContext *funcctx;

	check_shmem();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		void	   *tmp_array;
		Size		serialized_size;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Receive profile from shmq */
		profile = (Profile *) palloc0(sizeof(Profile));
		
		tmp_array = receive_array(PROFILE_REQUEST, &serialized_size,
								  &profile->count, &saved_profile_dimensions);
		profile->items = (ProfileItem *) deserialize_array(tmp_array, profile->count, false);
		funcctx->user_fctx = profile;
		funcctx->max_calls = profile->count;

		/* Make tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(15);
		fill_tuple_desc (tupdesc);
		TupleDescInitEntry(tupdesc, (AttrNumber) 15, "count",
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
		Datum		values[15];
		bool		nulls[15];
		HeapTuple	tuple;
		ProfileItem *item;

		item = &profile->items[funcctx->call_cntr];

		/* Make and return next tuple to caller */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		fill_values_and_nulls(values, nulls, item->dimensions, pgws_profile_dimensions);
		values[14] = UInt64GetDatum(item->count);
	
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

        /*
         * Check that the collector was started to avoid NULL
         * pointer dereference.
         */
	if (!pgws_collector_hdr->latch)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("pg_wait_sampling collector wasn't started")));

	SetLatch(pgws_collector_hdr->latch);

	LockRelease(&queueTag, ExclusiveLock, false);

	PG_RETURN_VOID();
}

//TODO OBSOLETE
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
		//history->items = (HistoryItem *) receive_array(HISTORY_REQUEST,
		//											   sizeof(HistoryItem), &history->count, &saved_history_dimensions);

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
		//HeapTuple	tuple;
		//HistoryItem *item;
		//Datum		values[5];
		//bool		nulls[5];
		//const char *event_type,
		//		   *event;
		//
		//item = &history->items[history->index];
		//
		///* Make and return next tuple to caller */
		//MemSet(values, 0, sizeof(values));
		//MemSet(nulls, 0, sizeof(nulls));
		//
		//event_type = pgstat_get_wait_event_type(item->dimensions.wait_event_info);
		//event = pgstat_get_wait_event(item->dimensions.wait_event_info);
		//values[0] = Int32GetDatum(item->dimensions.pid);
		//values[1] = TimestampTzGetDatum(item->ts);
		//if (event_type)
		//	values[2] = PointerGetDatum(cstring_to_text(event_type));
		//else
		//	nulls[2] = true;
		//if (event)
		//	values[3] = PointerGetDatum(cstring_to_text(event));
		//else
		//	nulls[3] = true;
		//
		//values[4] = UInt64GetDatum(item->dimensions.queryId);
		//tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		//
		//history->index++;
		//SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_history_extended);
Datum
pg_wait_sampling_get_history_extended(PG_FUNCTION_ARGS)
{
	History    *history;
	FuncCallContext *funcctx;
	void	   *tmp_array;

	check_shmem();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		Size		serialized_size;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Receive history from shmq */
		history = (History *) palloc0(sizeof(History));
		tmp_array = receive_array(HISTORY_REQUEST, &serialized_size,
								  &history->count, &saved_history_dimensions);
		history->items = (HistoryItem *) deserialize_array(tmp_array, history->count, true);
		funcctx->user_fctx = history;
		funcctx->max_calls = history->count;

		/* Make tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(15);
		fill_tuple_desc (tupdesc);
		TupleDescInitEntry(tupdesc, (AttrNumber) 15, "sample_ts", //XXX we have moved this to the end to have it more in line with current and profile; debatable; maybe move it to first place?
						   TIMESTAMPTZOID, -1, 0);
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
		Datum		values[15];
		bool		nulls[15];

		item = &history->items[history->index];

		/* Make and return next tuple to caller */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		fill_values_and_nulls(values, nulls, item->dimensions, pgws_history_dimensions);
		values[14] = TimestampTzGetDatum(item->ts); //XXX same as above

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
static void
pgws_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	int			i = MyProc - ProcGlobal->allProcs;

	if (pgws_enabled(nesting_level))
		pgws_proc_queryids[i] = queryDesc->plannedstmt->queryId;
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
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
