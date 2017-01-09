/*
 * pg_wait_sampling.c
 *		Track information about wait events.
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_wait_sampling/pg_wait_sampling.c
 */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/spin.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"

#include "pg_wait_sampling.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_fini(void);

/* Global variables */
bool					shmem_initialized = false;

/* Shared memory variables */
shm_toc				   *toc = NULL;
CollectorShmqHeader	   *collector_hdr = NULL;
shm_mq				   *collector_mq = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static PGPROC * search_proc(int backendPid);

/*
 * Estimate amount of shared memory needed.
 */
static Size
pgws_shmem_size(void)
{
	shm_toc_estimator	e;
	Size				size;
	int					nkeys;

	shm_toc_initialize_estimator(&e);

	nkeys = 2;

	shm_toc_estimate_chunk(&e, sizeof(CollectorShmqHeader));
	shm_toc_estimate_chunk(&e, (Size) COLLECTOR_QUEUE_SIZE);

	shm_toc_estimate_keys(&e, nkeys);
	size = shm_toc_estimate(&e);

	return size;
}

static bool
shmem_int_guc_check_hook(int *newval, void **extra, GucSource source)
{
	if (UsedShmemSegAddr == NULL)
		return false;
	return true;
}

static bool
shmem_bool_guc_check_hook(bool *newval, void **extra, GucSource source)
{
	if (UsedShmemSegAddr == NULL)
		return false;
	return true;
}

/*
 * This union allows us to mix the numerous different types of structs
 * that we are organizing.
 */
typedef union
{
	struct config_generic generic;
	struct config_bool _bool;
	struct config_real real;
	struct config_int integer;
	struct config_string string;
	struct config_enum _enum;
} mixedStruct;

/*
 * Setup new GUCs or modify existsing.
 */
static void
setup_gucs()
{
	struct config_generic **guc_vars;
	int			numOpts,
				i;
	bool		history_size_found = false,
				history_period_found = false,
				profile_period_found = false,
				profile_pid_found = false;

	guc_vars = get_guc_variables();
	numOpts = GetNumConfigOptions();

	for (i = 0; i < numOpts; i++)
	{
		mixedStruct *var = (mixedStruct *) guc_vars[i];
		const char *name = var->generic.name;

		if (var->generic.flags & GUC_CUSTOM_PLACEHOLDER)
			continue;

		if (!strcmp(name, "pg_wait_sampling.history_size"))
		{
			history_size_found = true;
			var->integer.variable = &collector_hdr->historySize;
			collector_hdr->historySize = 5000;
		}
		else if (!strcmp(name, "pg_wait_sampling.history_period"))
		{
			history_period_found = true;
			var->integer.variable = &collector_hdr->historyPeriod;
			collector_hdr->historyPeriod = 10;
		}
		else if (!strcmp(name, "pg_wait_sampling.profile_period"))
		{
			profile_period_found = true;
			var->integer.variable = &collector_hdr->profilePeriod;
			collector_hdr->profilePeriod = 10;
		}
		else if (!strcmp(name, "pg_wait_sampling.profile_pid"))
		{
			profile_pid_found = true;
			var->_bool.variable = &collector_hdr->profilePid;
			collector_hdr->profilePid = true;
		}
	}

	if (!history_size_found)
		DefineCustomIntVariable("pg_wait_sampling.history_size",
				"Sets size of waits history.", NULL,
				&collector_hdr->historySize, 5000, 100, INT_MAX,
				PGC_SUSET, 0, shmem_int_guc_check_hook, NULL, NULL);

	if (!history_period_found)
		DefineCustomIntVariable("pg_wait_sampling.history_period",
				"Sets period of waits history sampling.", NULL,
				&collector_hdr->historyPeriod, 10, 1, INT_MAX,
				PGC_SUSET, 0, shmem_int_guc_check_hook, NULL, NULL);

	if (!profile_period_found)
		DefineCustomIntVariable("pg_wait_sampling.profile_period",
				"Sets period of waits profile sampling.", NULL,
				&collector_hdr->profilePeriod, 10, 1, INT_MAX,
				PGC_SUSET, 0, shmem_int_guc_check_hook, NULL, NULL);

	if (!profile_pid_found)
		DefineCustomBoolVariable("pg_wait_sampling.profile_pid",
				"Sets whether profile should be collected per pid.", NULL,
				&collector_hdr->profilePid, true,
				PGC_SUSET, 0, shmem_bool_guc_check_hook, NULL, NULL);

	if (history_size_found || history_period_found
		|| profile_period_found || profile_pid_found)
		ProcessConfigFile(PGC_SIGHUP);
}

/*
 * Distribute shared memory.
 */
static void
pgws_shmem_startup(void)
{
	bool	found;
	Size	segsize = pgws_shmem_size();
	void   *pgws;

	pgws = ShmemInitStruct("pg_wait_sampling", segsize, &found);

	if (!found)
	{
		toc = shm_toc_create(PG_WAIT_SAMPLING_MAGIC, pgws, segsize);

		collector_hdr = shm_toc_allocate(toc, sizeof(CollectorShmqHeader));
		shm_toc_insert(toc, 0, collector_hdr);
		collector_mq = shm_toc_allocate(toc, COLLECTOR_QUEUE_SIZE);
		shm_toc_insert(toc, 1, collector_mq);

		/* Initialize GUC variables in shared memory */
		setup_gucs();
	}
	else
	{
		toc = shm_toc_attach(PG_WAIT_SAMPLING_MAGIC, pgws);

		collector_hdr = shm_toc_lookup(toc, 0);
		collector_mq = shm_toc_lookup(toc, 1);
	}

	shmem_initialized = true;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
}

/*
 * Check shared memory is initialized. Report an error otherwise.
 */
void
check_shmem(void)
{
	if (!shmem_initialized)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("pg_wait_sampling shared memory wasn't initialized yet")));
	}
}

/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgws_shmem_startup().
	 */
	RequestAddinShmemSpace(pgws_shmem_size());

	register_wait_collector();

	/*
	 * Install hooks.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgws_shmem_startup;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
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
		WaitCurrentContext 	*params;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		params = (WaitCurrentContext *)palloc0(sizeof(WaitCurrentContext));
		params->ts = GetCurrentTimestamp();

		funcctx->user_fctx = params;
		tupdesc = CreateTemplateTupleDesc(3, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "event",
						   TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		LWLockAcquire(ProcArrayLock, LW_SHARED);

		if (!PG_ARGISNULL(0))
		{
			HistoryItem	   *item;
			PGPROC		   *proc;

			proc = search_proc(PG_GETARG_UINT32(0));
			if (proc->wait_event_info != 0)
			{
				params->items = (HistoryItem *) palloc0(sizeof(HistoryItem));
				item = &params->items[0];
				item->pid = proc->pid;
				item->wait_event_info = proc->wait_event_info;
				funcctx->max_calls = 1;
			}
			else
			{
				funcctx->max_calls = 0;
			}
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

				if (proc != NULL && proc->pid != 0 && proc->wait_event_info != 0)
				{
					params->items[j].pid = proc->pid;
					params->items[j].wait_event_info = proc->wait_event_info;
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
		Datum		values[3];
		bool		nulls[3];
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

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

typedef struct
{
	Size			count;
	ProfileItem	   *items;
} Profile;

static void
init_lock_tag(LOCKTAG *tag)
{
	tag->locktag_field1 = PG_WAIT_SAMPLING_MAGIC;
	tag->locktag_field2 = 0;
	tag->locktag_field3 = 0;
	tag->locktag_field4 = 0;
	tag->locktag_type = LOCKTAG_USERLOCK;
	tag->locktag_lockmethodid = USER_LOCKMETHOD;
}

static void *
receive_array(SHMRequest request, Size item_size, Size *count)
{
	LOCKTAG			tag;
	shm_mq		   *mq;
	shm_mq_handle  *mqh;
	shm_mq_result	res;
	Size			len,
					i;
	void		   *data;
	Pointer			result,
					ptr;

	init_lock_tag(&tag);
	LockAcquire(&tag, ExclusiveLock, false, false);

	mq = shm_mq_create(collector_mq, COLLECTOR_QUEUE_SIZE);
	collector_hdr->request = request;

	if (!collector_hdr->latch)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("pg_wait_sampling collector wasn't started")));

	SetLatch(collector_hdr->latch);

	shm_mq_set_receiver(mq, MyProc);
	mqh = shm_mq_attach(mq, NULL, NULL);

	res = shm_mq_receive(mqh, &len, &data, false);
	if (res != SHM_MQ_SUCCESS || len != sizeof(*count))
		elog(ERROR, "Error reading mq.");
	memcpy(count, data, sizeof(*count));

	result = palloc(item_size * (*count));
	ptr = result;

	for (i = 0; i < *count; i++)
	{
		res = shm_mq_receive(mqh, &len, &data, false);
		if (res != SHM_MQ_SUCCESS || len != item_size)
			elog(ERROR, "Error reading mq.");
		memcpy(ptr, data, item_size);
		ptr += item_size;
	}

	shm_mq_detach(mq);

	LockRelease(&tag, ExclusiveLock, false);

	return result;
}


PG_FUNCTION_INFO_V1(pg_wait_sampling_get_profile);
Datum
pg_wait_sampling_get_profile(PG_FUNCTION_ARGS)
{
	Profile			   *profile;
	FuncCallContext	   *funcctx;

	check_shmem();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext		oldcontext;
		TupleDesc			tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Receive profile from shmq */
		profile = (Profile *) palloc0(sizeof(Profile));
		profile->items = (ProfileItem *) receive_array(PROFILE_REQUEST,
										sizeof(ProfileItem), &profile->count);

		funcctx->user_fctx = profile;
		funcctx->max_calls = profile->count;

		/* Make tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(4, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "event",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "count",
						   INT8OID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	profile = (Profile *)funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		/* for each row */
		Datum		values[4];
		bool		nulls[4];
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
		values[3] = Int64GetDatum(item->count);

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
	LOCKTAG		tag;

	check_shmem();

	init_lock_tag(&tag);

	LockAcquire(&tag, ExclusiveLock, false, false);

	collector_hdr->request = PROFILE_RESET;
	SetLatch(collector_hdr->latch);

	LockRelease(&tag, ExclusiveLock, false);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_wait_sampling_get_history);
Datum
pg_wait_sampling_get_history(PG_FUNCTION_ARGS)
{
	History				*history;
	FuncCallContext		*funcctx;

	check_shmem();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcontext;
		TupleDesc		tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Receive history from shmq */
		history = (History *) palloc0(sizeof(History));
		history->items = (HistoryItem *) receive_array(HISTORY_REQUEST,
										sizeof(HistoryItem), &history->count);

		funcctx->user_fctx = history;
		funcctx->max_calls = history->count;

		/* Make tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(4, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "sample_ts",
						   TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "event",
						   TEXTOID, -1, 0);
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
		Datum		values[4];
		bool		nulls[4];
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
