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
									  int cursorOptions, ParamListInfo boundParams
#if PG_VERSION_NUM >= 190000
									  , ExplainState *es
#endif
									  );
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

/* Like in pg_stat_statements */
typedef enum pgwsVersion
{
	PGWS_V1_1 = 0,
	PGWS_V1_2,
} pgwsVersion;

Datum pg_wait_sampling_get_current_internal(FunctionCallInfo fcinfo,
											pgwsVersion api_version);
Datum pg_wait_sampling_get_profile_internal(FunctionCallInfo fcinfo,
											pgwsVersion api_version);
Datum pg_wait_sampling_get_history_internal(FunctionCallInfo fcinfo,
											pgwsVersion api_version);

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
	int			dimensions_mask = 0;
	int		   *myextra;

	/* Check special case when we turn all dimensions */
	if (pg_strcasecmp(*newvalue, "all") == 0)
		dimensions_mask = PGWS_DIMENSIONS_ALL;
	else
	{
		/* Empty strings are not allowed */
		if (strlen(*newvalue) == 0)
		{
			GUC_check_errdetail("Empty string is not allowed");
			return false;
		}

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
				dimensions_mask |= PGWS_DIMENSIONS_PID;
			else if (pg_strcasecmp(tok, "event") == 0)
			{
				dimensions_mask |= PGWS_DIMENSIONS_WAIT_EVENT;
			}
			else if (pg_strcasecmp(tok, "queryid") == 0)
				dimensions_mask |= PGWD_DIMENSIONS_QUERY_ID;
			else if (pg_strcasecmp(tok, "role_id") == 0)
				dimensions_mask |= PGWS_DIMENSIONS_ROLE_ID;
			else if (pg_strcasecmp(tok, "database_id") == 0)
				dimensions_mask |= PGWS_DIMENSIONS_DB_ID;
			else if (pg_strcasecmp(tok, "leader_pid") == 0)
				dimensions_mask |= PGWS_DIMENSIONS_PARALLEL_LEADER_PID;
			else if (pg_strcasecmp(tok, "is_regular_backend") == 0)
				dimensions_mask |= PGWS_DIMENSIONS_IS_REGULAR_BE;
			else if (pg_strcasecmp(tok, "all") == 0)
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
	*myextra = dimensions_mask;
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
							   "pid, event, queryid",
							   PGC_SIGHUP,
							   GUC_LIST_INPUT,
							   pgws_general_dimensions_check_hook,
							   pgws_history_dimensions_assign_hook,
							   NULL);

	DefineCustomStringVariable("pg_wait_sampling.profile_dimensions",
							   "Sets sampling dimensions for profile",
							   NULL,
							   &pgws_profile_dimensions_string,
							   "pid, event, queryid",
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
	Sample		*samples;
	TimestampTz	ts;
} WaitCurrentContext;

/* Like in pg_stat_statements */
#define PG_WAIT_SAMPLING_COLS_V1_1	5
#define PG_WAIT_SAMPLING_COLS_V1_2	9
#define PG_WAIT_SAMPLING_COLS		9	/* maximum of above */

/*
 * Common routine to fill "dimensions" part of tupdesc
 */
static void
fill_tuple_desc (TupleDesc tupdesc, pgwsVersion api_version)
{
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "type",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "event",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "queryid",
					   INT8OID, -1, 0);
	if (api_version >= PGWS_V1_2)
	{
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "role_id",
						INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "database_id",
						INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "leader_pid",
						INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "is_regular_backend",
						BOOLOID, -1, 0);
	}
}

static void
fill_values_and_nulls(Datum *values, bool *nulls, Sample sample,
					  int dimensions_mask, pgwsVersion api_version)
{
	if (dimensions_mask & PGWS_DIMENSIONS_PID)
		values[0] = Int32GetDatum(sample.pid);
	else
		nulls[0] = true;
	if (sample.wait_event_info != 0 && (dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT))
	{
		values[1] = PointerGetDatum(cstring_to_text(pgstat_get_wait_event_type(sample.wait_event_info)));
		values[2] = PointerGetDatum(cstring_to_text(pgstat_get_wait_event(sample.wait_event_info)));
	}
	else
	{
		nulls[1] = true;
		nulls[2] = true;
	}
	if (pgws_profileQueries || (dimensions_mask & PGWD_DIMENSIONS_QUERY_ID))
		values[3] = UInt64GetDatum(sample.queryId);
	else
		nulls[3] = true;
	if (api_version >= PGWS_V1_2)
	{
		if (dimensions_mask & PGWS_DIMENSIONS_ROLE_ID)
			values[4] = ObjectIdGetDatum(sample.role_id);
		else
			nulls[4] = true;
		if (dimensions_mask & PGWS_DIMENSIONS_DB_ID)
			values[5] = ObjectIdGetDatum(sample.database_id);
		else
			nulls[5] = true;
		if (dimensions_mask & PGWS_DIMENSIONS_PARALLEL_LEADER_PID)
			values[6] = Int32GetDatum(sample.parallel_leader_pid);
		else
			nulls[6] = true;
		if (dimensions_mask & PGWS_DIMENSIONS_IS_REGULAR_BE)
			values[7] = BoolGetDatum(sample.is_regular_backend);
		else
			nulls[7] = true;
	}
}

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_current);
Datum
pg_wait_sampling_get_current(PG_FUNCTION_ARGS)
{
	return pg_wait_sampling_get_current_internal(fcinfo, PGWS_V1_1);
}

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_current_1_2);
Datum
pg_wait_sampling_get_current_1_2(PG_FUNCTION_ARGS)
{
	return pg_wait_sampling_get_current_internal(fcinfo, PGWS_V1_2);
}

Datum
pg_wait_sampling_get_current_internal(FunctionCallInfo fcinfo,
									  pgwsVersion api_version)
{
	FuncCallContext *funcctx;
	WaitCurrentContext *params;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	check_shmem();

	/*
	 * Check we have the expected number of output arguments. Safety check
	 *
	 * +1 is because pg_wait_sampling_current doesn't have count/ts column
	 */
	switch(api_version)
	{
		case PGWS_V1_1:
			if (rsinfo->expectedDesc->natts + 1 != PG_WAIT_SAMPLING_COLS_V1_1)
				elog(ERROR, "incorrect number of output arguments");
			break;
		case PGWS_V1_2:
			if (rsinfo->expectedDesc->natts + 1 != PG_WAIT_SAMPLING_COLS_V1_2)
				elog(ERROR, "incorrect number of output arguments");
			break;
		default:
			elog(ERROR, "incorrect number of output arguments");
	}

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
		tupdesc = CreateTemplateTupleDesc(rsinfo->expectedDesc->natts);
		fill_tuple_desc (tupdesc, api_version);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		LWLockAcquire(ProcArrayLock, LW_SHARED);

		if (!PG_ARGISNULL(0))
		{
			/* pg_wait_sampling_get_current(pid int4) function */
			Sample		*sample;
			PGPROC		*proc;

			proc = search_proc(PG_GETARG_UINT32(0));
			params->samples = (Sample *) palloc0(sizeof(Sample));
			sample = &params->samples[0];
			fill_sample(sample, proc, proc->pid, proc->wait_event_info, //TODO should we save pid and wait_event_info like in probe_waits?
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

			params->samples = (Sample *) palloc0(sizeof(Sample) * procCount);
			for (i = 0; i < procCount; i++)
			{
				PGPROC	   *proc = &ProcGlobal->allProcs[i];

				if (!pgws_should_sample_proc(proc,
											 &params->samples[j].pid,
											 &params->samples[j].wait_event_info))
					continue;

				fill_sample(&params->samples[j], proc, proc->pid, proc->wait_event_info, //TODO should we save pid and wait_event_info like in probe_waits?
							pgws_proc_queryids[proc - ProcGlobal->allProcs],
							PGWS_DIMENSIONS_ALL);
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
		Datum		values[PG_WAIT_SAMPLING_COLS - 1];
		bool		nulls[PG_WAIT_SAMPLING_COLS - 1];
		Sample		*sample;

		sample = &params->samples[funcctx->call_cntr];

		/* Make and return next tuple to caller */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		fill_values_and_nulls(values, nulls, *sample, PGWS_DIMENSIONS_ALL, api_version);

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
	Sample		*samples;
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
	void	   *data,
			   *result;
	char	   *ptr;
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
	return pg_wait_sampling_get_profile_internal(fcinfo, PGWS_V1_1);
}

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_profile_1_2);
Datum
pg_wait_sampling_get_profile_1_2(PG_FUNCTION_ARGS)
{
	return pg_wait_sampling_get_profile_internal(fcinfo, PGWS_V1_2);
}

Datum
pg_wait_sampling_get_profile_internal(FunctionCallInfo fcinfo,
									  pgwsVersion api_version)
{
	Profile			*profile;
	FuncCallContext	*funcctx;
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	check_shmem();

	/*
	 * Check we have the expected number of output arguments. Safety check
	 */
	switch(api_version)
	{
		case PGWS_V1_1:
			if (rsinfo->expectedDesc->natts != PG_WAIT_SAMPLING_COLS_V1_1)
				elog(ERROR, "incorrect number of output arguments");
			break;
		case PGWS_V1_2:
			if (rsinfo->expectedDesc->natts != PG_WAIT_SAMPLING_COLS_V1_2)
				elog(ERROR, "incorrect number of output arguments");
			break;
		default:
			elog(ERROR, "incorrect number of output arguments");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Receive profile from shmq */
		profile = (Profile *) palloc0(sizeof(Profile));
		profile->samples = (Sample *) receive_array(PROFILE_REQUEST,
													sizeof(Sample), &profile->count);

		funcctx->user_fctx = profile;
		funcctx->max_calls = profile->count;

		/* Make tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(rsinfo->expectedDesc->natts);
		fill_tuple_desc (tupdesc, api_version);
		TupleDescInitEntry(tupdesc, (AttrNumber) rsinfo->expectedDesc->natts, "count",
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
		Datum		values[PG_WAIT_SAMPLING_COLS];
		bool		nulls[PG_WAIT_SAMPLING_COLS];
		HeapTuple	tuple;
		Sample		*sample;

		sample = &profile->samples[funcctx->call_cntr];

		/* Make and return next tuple to caller */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		fill_values_and_nulls(values, nulls, *sample,
							  pgws_profile_dimensions, api_version);
		values[rsinfo->expectedDesc->natts - 1] = UInt64GetDatum(sample->count);

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

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_history);
Datum
pg_wait_sampling_get_history(PG_FUNCTION_ARGS)
{
	return pg_wait_sampling_get_history_internal(fcinfo, PGWS_V1_1);
}

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_history_1_2);
Datum
pg_wait_sampling_get_history_1_2(PG_FUNCTION_ARGS)
{
	return pg_wait_sampling_get_history_internal(fcinfo, PGWS_V1_2);
}

Datum
pg_wait_sampling_get_history_internal(FunctionCallInfo fcinfo,
									  pgwsVersion api_version)
{
	History    *history;
	FuncCallContext *funcctx;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	check_shmem();

	/*
	 * Check we have the expected number of output arguments. Safety check
	 */
	switch(api_version)
	{
		case PGWS_V1_1:
			if (rsinfo->expectedDesc->natts != PG_WAIT_SAMPLING_COLS_V1_1)
				elog(ERROR, "incorrect number of output arguments");
			break;
		case PGWS_V1_2:
			if (rsinfo->expectedDesc->natts != PG_WAIT_SAMPLING_COLS_V1_2)
				elog(ERROR, "incorrect number of output arguments");
			break;
		default:
			elog(ERROR, "incorrect number of output arguments");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Receive history from shmq */
		history = (History *) palloc0(sizeof(History));
		history->samples = (Sample *) receive_array(HISTORY_REQUEST,
													sizeof(Sample), &history->count);

		funcctx->user_fctx = history;
		funcctx->max_calls = history->count;

		/* Make tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(rsinfo->expectedDesc->natts);
		fill_tuple_desc (tupdesc, api_version);
		TupleDescInitEntry(tupdesc, (AttrNumber) rsinfo->expectedDesc->natts, "sample_ts",
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
		Sample		*sample;
		Datum		values[PG_WAIT_SAMPLING_COLS];
		bool		nulls[PG_WAIT_SAMPLING_COLS];

		sample = &history->samples[history->index];

		/* Make and return next tuple to caller */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		fill_values_and_nulls(values, nulls, *sample,
							  pgws_history_dimensions, api_version);
		values[rsinfo->expectedDesc->natts - 1] = TimestampTzGetDatum(sample->ts);
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
				  ParamListInfo boundParams
#if PG_VERSION_NUM >= 190000
				  , ExplainState *es
#endif
				  )
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
									   cursorOptions, boundParams
#if PG_VERSION_NUM >= 190000
									   , es
#endif
									   );
		else
			result = standard_planner(parse,
#if PG_VERSION_NUM >= 130000
									  query_string,
#endif
									  cursorOptions, boundParams
#if PG_VERSION_NUM >= 190000
									  , es
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
