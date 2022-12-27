/*
 * pg_wait_sampling.c
 *		Track information about wait events.
 *
 * Copyright (c) 2015-2017, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_wait_sampling/pg_wait_sampling.c
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker.h"
#if PG_VERSION_NUM >= 120000
#include "replication/walsender.h"
#endif
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#if PG_VERSION_NUM >= 140000
#include "utils/queryjumble.h"
#endif
#include "utils/memutils.h" /* TopMemoryContext.  Actually for PG 9.6 only,
							 * but there should be no harm for others. */

#include "compat.h"
#include "pg_wait_sampling.h"

PG_MODULE_MAGIC;

/* Marker whether extension is setup in shared mode */
static bool shmem_initialized = false;

/* Global settings */
int MaxProfileEntries = 5000;
int HistoryBufferSize = 5000;
int HistoryPeriod = 0;
int ProfilePeriod = 10;
bool WhetherProfilePid = true;
bool WhetherProfileQueryId = true;

/* Function declarations */
void _PG_init(void);
// TODO: add void _PG_fini(void);

/* Hooks */
static ExecutorEnd_hook_type	prev_ExecutorEnd = NULL;
static planner_hook_type		planner_hook_next = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type 	prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type	prev_shmem_startup_hook = NULL;
static PlannedStmt *pgws_planner_hook(Query *parse,
#if PG_VERSION_NUM >= 130000
		const char *query_string,
#endif
		int cursorOptions, ParamListInfo boundParams);
static void pgws_ExecutorEnd(QueryDesc *queryDesc);

/* Pointers to shared memory objects */
pgwsQueryId *pgws_proc_queryids = NULL;
HTAB		*pgws_hash = NULL;
LWLock		*pgws_hash_lock = NULL;
History		*pgws_history_ring = NULL;
LWLock		*pgws_history_lock = NULL;

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
	int count = 0;

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
	 * On older versions, we need to compute MaxBackends: bgworkers, autovacuum
	 * workers and launcher.
	 * This has to be in sync with the value computed in
	 * InitializeMaxBackends() (postinit.c)
	 *
	 * Note that we need to calculate the value as it won't initialized when we
	 * need it during _PG_init().
	 *
	 * Note also that the value returned during _PG_init() might be different
	 * from the value returned later if some third-party modules change one of
	 * the underlying GUC.  This isn't ideal but can't lead to a crash, as the
	 * value returned during _PG_init() is only used to ask for additional
	 * shmem with RequestAddinShmemSpace(), and postgres has an extra 100kB of
	 * shmem to compensate some small unaccounted usage.  So if the value later
	 * changes, we will allocate and initialize the new (and correct) memory
	 * size, which will either work thanks for the extra 100kB of shmem, of
	 * fail (and prevent postgres startup) due to an out of shared memory
	 * error.
	 */
	count += MaxConnections + autovacuum_max_workers + 1
			+ max_worker_processes;

	/*
	 * Starting with pg12, wal senders aren't part of MaxConnections anymore
	 * and have to be accounted for.
	 */
#if PG_VERSION_NUM >= 120000
	count += max_wal_senders;
#endif		/* pg 12+ */
#endif		/* pg 15- */
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
	Size size = 0;

	size = add_size(size, sizeof(pgwsQueryId) * get_max_procs_count());
	size = add_size(size, hash_estimate_size(MaxProfileEntries,
											 sizeof(ProfileHashKey)));
	size = add_size(size,
					sizeof(History) + sizeof(HistoryItem) * HistoryBufferSize);

	return size;
}

static void
pgwsEnableQueryId(bool newval, void *extra)
{
#if PG_VERSION_NUM >= 140000
	if (newval)
		EnableQueryId();
#endif
}

/*
 * Setup new GUCs or modify existsing.
 */
static void
setup_gucs()
{
	DefineCustomIntVariable("pg_wait_sampling.max_profile_entries",
			"Sets maximum number of entries in bounded profile table.", NULL,
			&MaxProfileEntries, 5000, 100, INT_MAX,
			PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("pg_wait_sampling.history_size",
			"Sets size for ring buffer for waits history in bytes.", NULL,
			&HistoryBufferSize, 5000, 100, INT_MAX,
			PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("pg_wait_sampling.history_period",
			"Sets period of waits history sampling in milliseconds.",
			"0 disables history populating.",
			&HistoryPeriod, 0, 0, INT_MAX,
			PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("pg_wait_sampling.profile_period",
			"Sets period of waits profile sampling in milliseconds.",
			"0 disables profiling.",
			&ProfilePeriod, 10, 0, INT_MAX,
			PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_wait_sampling.profile_pid",
			"Sets whether profile should be collected per pid.", NULL,
			&WhetherProfilePid, true,
			PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_wait_sampling.profile_queries",
			"Sets whether profile should be collected per query.", NULL,
			&WhetherProfileQueryId, true,
			PGC_POSTMASTER, 0, NULL, pgwsEnableQueryId, NULL);
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
	RequestNamedLWLockTranche("pg_wait_sampling", 2);
}
#endif

/*
 * Distribute shared memory.
 */
static void
pgws_shmem_startup(void)
{
	bool	found;
	HASHCTL	info;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pgws_proc_queryids = ShmemInitStruct(
			"pg_wait_sampling queryids",
			sizeof(pgwsQueryId) * get_max_procs_count(),
			&found);
	MemSet(pgws_proc_queryids, 0, sizeof(pgwsQueryId) * get_max_procs_count());
	if (!found)
	{
		/* First time through ... */
		LWLockPadded *locks = GetNamedLWLockTranche("pg_wait_sampling");

		pgws_hash_lock = &(locks[0]).lock;
		pgws_history_lock = &(locks[1]).lock;
	}

	pgws_history_ring = ShmemInitStruct(
			"pg_wait_sampling history ring",
			sizeof(History) + sizeof(HistoryItem) * HistoryBufferSize,
			&found);
	pgws_history_ring->index = 0;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ProfileHashKey);
	info.entrysize = sizeof(ProfileHashEntry);
	pgws_hash = ShmemInitHash("pg_wait_sampling hash",
							  MaxProfileEntries, MaxProfileEntries,
							  &info, HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);

	shmem_initialized = true;
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

/*
 * Register background worker for collecting waits history.
 */
static void
pgws_register_wait_collector(void)
{
	BackgroundWorker worker;

	/* Set up background worker parameters */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 1;
	worker.bgw_notify_pid = 0;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_wait_sampling");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, CppAsString(pgws_collector_main));
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_wait_sampling collector");
	worker.bgw_main_arg = (Datum) 0;
	RegisterBackgroundWorker(&worker);
}


/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	setup_gucs();

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
	RequestNamedLWLockTranche("pg_wait_sampling", 2);
#endif

	pgws_register_wait_collector();

	/*
	 * Install hooks.
	 */
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook		= pgws_shmem_request;
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook		= pgws_shmem_startup;
	planner_hook_next		= planner_hook;
	planner_hook			= pgws_planner_hook;
	prev_ExecutorEnd		= ExecutorEnd_hook;
	ExecutorEnd_hook		= pgws_ExecutorEnd;
}

/*
 * Find PGPROC entry responsible for given pid assuming ProcArrayLock was
 * already taken.
 */
static PGPROC *
search_proc(int pid)
{
	int i;

	if (pid == 0)
		return MyProc;

	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		PGPROC	*proc = &ProcGlobal->allProcs[i];
		if (proc->pid && proc->pid == pid)
		{
			return proc;
		}
	}

	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("backend with pid=%d not found", pid)));
	return NULL;
}

typedef struct
{
	HistoryItem	   *items;
	TimestampTz		ts;
} WaitCurrentContext;

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_current);
Datum
pg_wait_sampling_get_current(PG_FUNCTION_ARGS)
{
	FuncCallContext 	*funcctx;
	WaitCurrentContext 	*params;

	check_shmem();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext		oldcontext;
		TupleDesc			tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		params = (WaitCurrentContext *)palloc0(sizeof(WaitCurrentContext));
		params->ts = GetCurrentTimestamp();

		funcctx->user_fctx = params;
		tupdesc = CreateTemplateTupleDescCompat(4, false);
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
			HistoryItem	   *item;
			PGPROC		   *proc;

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
			int		procCount = ProcGlobal->allProcCount,
					i,
					j = 0;

			params->items = (HistoryItem *) palloc0(sizeof(HistoryItem) * procCount);
			for (i = 0; i < procCount; i++)
			{
				PGPROC *proc = &ProcGlobal->allProcs[i];

				if (proc != NULL && proc->pid != 0 && proc->wait_event_info)
				{
					params->items[j].pid = proc->pid;
					params->items[j].wait_event_info = proc->wait_event_info;
					params->items[j].queryId = pgws_proc_queryids[i];
					j++;
				}
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
		SRF_RETURN_DONE(funcctx);
	}
}


PG_FUNCTION_INFO_V1(pg_wait_sampling_get_profile);
Datum
pg_wait_sampling_get_profile(PG_FUNCTION_ARGS)
{
	ProfileHashEntry	*profile;
	FuncCallContext		*funcctx;

	check_shmem();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext		oldcontext;
		TupleDesc			tupdesc;
		HASH_SEQ_STATUS		hash_seq;
		ProfileHashEntry   *entry;
		int					profile_count,
							entry_index;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Extract profile from shared memory */
		profile_count = hash_get_num_entries(pgws_hash);
		profile = (ProfileHashEntry *)
			palloc(sizeof(ProfileHashEntry) * profile_count);

		entry_index = 0;
		LWLockAcquire(pgws_hash_lock, LW_SHARED);
		hash_seq_init(&hash_seq, pgws_hash);
		while ((entry = hash_seq_search(&hash_seq)) != NULL)
		{
			profile[entry_index++] = *entry;
		}
		LWLockRelease(pgws_hash_lock);

		/* Build result rows */
		funcctx->user_fctx = profile;
		funcctx->max_calls = profile_count;

		/* Make tuple descriptor */
		tupdesc = CreateTemplateTupleDescCompat(5, false);
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

	profile = (ProfileHashEntry *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		/* for each row */
		Datum		values[5];
		bool		nulls[5];
		HeapTuple	tuple;
		ProfileHashEntry *item;
		const char *event_type,
				   *event;

		item = &profile[funcctx->call_cntr];

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		/* Make and return next tuple to caller */
		event_type = pgstat_get_wait_event_type(item->key.wait_event_info);
		event = pgstat_get_wait_event(item->key.wait_event_info);
		if (WhetherProfilePid)
			values[0] = Int32GetDatum(item->key.pid);
		else
			nulls[0] = true;
		if (event_type)
			values[1] = PointerGetDatum(cstring_to_text(event_type));
		else
			nulls[1] = true;
		if (event)
			values[2] = PointerGetDatum(cstring_to_text(event));
		else
			nulls[2] = true;

		if (WhetherProfileQueryId)
			values[3] = UInt64GetDatum(item->key.queryid);
		else
			nulls[3] = true;

		values[4] = UInt64GetDatum(item->counter);

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
	HASH_SEQ_STATUS		hash_seq;
	ProfileHashEntry   *entry;

	check_shmem();

	LWLockAcquire(pgws_hash_lock, LW_EXCLUSIVE);

	/* Remove all profile entries. */
	hash_seq_init(&hash_seq, pgws_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(pgws_hash, &entry->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(pgws_hash_lock);

	/*
	 * TODO: consider saving of the time of statistics reset to more easly
	 * compute the differential counters. It might look as global time
	 * accessable via separate function call as it's done in pg_stat_statemens
	 * or more granular time accounting per profile entries to take into account
	 * evictions of these entries from restricted by size hashtable.
	 */

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_history);
Datum
pg_wait_sampling_get_history(PG_FUNCTION_ARGS)
{
	HistoryItem			*history;
	FuncCallContext		*funcctx;

	check_shmem();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcontext;
		TupleDesc		tupdesc;
		int				history_size;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Extract history from shared ring buffer */
		LWLockAcquire(pgws_history_lock, LW_SHARED);

		history_size = pgws_history_ring->index < HistoryBufferSize ?
			pgws_history_ring->index : HistoryBufferSize;
		history = (HistoryItem *) palloc(history_size * sizeof(HistoryItem));
		memcpy(history, pgws_history_ring->items,
			   history_size * sizeof(HistoryItem));

		LWLockRelease(pgws_history_lock);

		/* Save function context */
		funcctx->user_fctx = history;
		funcctx->max_calls = history_size;

		/* Make tuple descriptor */
		tupdesc = CreateTemplateTupleDescCompat(5, false);
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

	history = (HistoryItem *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		HeapTuple	tuple;
		HistoryItem *item;
		Datum		values[5];
		bool		nulls[5];
		const char *event_type,
				   *event;

		item = &history[funcctx->call_cntr];

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
	if (MyProc)
	{
		int i = MyProc - ProcGlobal->allProcs;

		StaticAssertExpr(sizeof(parse->queryId) == sizeof(pgwsQueryId),
						 "queryId size is not correct");
		if (!pgws_proc_queryids[i])
			pgws_proc_queryids[i] = parse->queryId;
	}

	/* Invoke original hook if needed */
	if (planner_hook_next)
		return planner_hook_next(parse,
#if PG_VERSION_NUM >= 130000
				query_string,
#endif
				cursorOptions, boundParams);

	return standard_planner(parse,
#if PG_VERSION_NUM >= 130000
				query_string,
#endif
			cursorOptions, boundParams);
}

/*
 * ExecutorEnd hook: clear queryId
 */
static void
pgws_ExecutorEnd(QueryDesc *queryDesc)
{
	if (MyProc)
		pgws_proc_queryids[MyProc - ProcGlobal->allProcs] = UINT64CONST(0);

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}
