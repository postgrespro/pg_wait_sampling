/*
 * collector.c
 *		Collector of wait event history and profile.
 *
 * Copyright (c) 2015-2025, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_wait_sampling/pg_wait_sampling.c
 */
#include "postgres.h"

#include <signal.h>
#include <time.h>

#include "compat.h"
#include "miscadmin.h"
#include "pg_wait_sampling.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shm_mq.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

#define check_bestatus_dimensions(dimensions) \
					   (dimensions & (PGWS_DIMENSIONS_BE_TYPE |\
									  PGWS_DIMENSIONS_BE_STATE |\
									  PGWS_DIMENSIONS_BE_START_TIME |\
									  PGWS_DIMENSIONS_CLIENT_ADDR |\
									  PGWS_DIMENSIONS_CLIENT_HOSTNAME |\
									  PGWS_DIMENSIONS_APPNAME))
static volatile sig_atomic_t shutdown_requested = false;

int saved_profile_dimensions;
int saved_history_dimensions;

static void handle_sigterm(SIGNAL_ARGS);

/*
 * Register background worker for collecting waits history.
 */
void
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
 * Allocate memory for waits history.
 */
static void
alloc_history(History *observations, int count)
{
	int serialized_size;

	saved_history_dimensions = pgws_history_dimensions;
	serialized_size = get_serialized_size(saved_history_dimensions, true);

	observations->serialized_items = (char *) palloc0(serialized_size * count);
	observations->index = 0;
	observations->count = count;
	observations->wraparound = false;
}

/*
 * Reallocate memory for changed number of history items.
 */
static void
realloc_history(History *observations, int count)
{
	char	   *newitems;
	int			copyCount,
				i,
				j;
	int			serialized_size;

	serialized_size = get_serialized_size(saved_history_dimensions, true);

	/* Allocate new array for history */
	newitems = (char *) palloc0(serialized_size * count);

	/* Copy entries from old array to the new */
	if (observations->wraparound)
		copyCount = observations->count;
	else
		copyCount = observations->index;

	copyCount = Min(copyCount, count);

	i = 0;
	if (observations->wraparound)
		j = observations->index + 1;
	else
		j = 0;
	while (i < copyCount)
	{
		if (j >= observations->count)
			j = 0;
		memcpy((newitems + i * serialized_size),
			   (observations->serialized_items + j * serialized_size),
				serialized_size);
		i++;
		j++;
	}

	/* Switch to new history array */
	pfree(observations->serialized_items);
	observations->serialized_items = newitems;
	observations->index = copyCount;
	observations->count = count;
	observations->wraparound = false;
}

static void
handle_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	shutdown_requested = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

/*
 * Get next item of history with rotation.
 */
static char *
get_next_observation(History *observations)
{
	char		*result;
	int			 serialized_size = get_serialized_size(saved_history_dimensions, true);

	/* Check for wraparound */
	if (observations->index >= observations->count)
	{
		observations->index = 0;
		observations->wraparound = true;
	}
	result = &observations->serialized_items[observations->index * serialized_size];
	observations->index++;
	return result;
}

void
fill_dimensions(SamplingDimensions *dimensions, PGPROC *proc,
				int pid, uint32 wait_event_info, uint64 queryId,
				int dimensions_mask)
{
	Oid		role_id = proc->roleId;
	Oid		database_id = proc->databaseId;
	PGPROC *lockGroupLeader = proc->lockGroupLeader;
#if PG_VERSION_NUM >= 180000
	bool	is_regular_backend = proc->isRegularBackend;
#else
	bool	is_regular_backend = !proc->isBackgroundWorker;
#endif

	if (dimensions_mask & PGWS_DIMENSIONS_PID)
		dimensions->pid = pid;

	if (dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT_TYPE ||
		dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT)
		dimensions->wait_event_info = wait_event_info;

	if (pgws_profileQueries || (dimensions_mask & PGWD_DIMENSIONS_QUERY_ID))
		dimensions->queryId = queryId;

	/* Copy everything we need from PGPROC */
	if (dimensions_mask & PGWS_DIMENSIONS_ROLE_ID)
		dimensions->role_id = role_id;

	if (dimensions_mask & PGWS_DIMENSIONS_DB_ID)
		dimensions->database_id = database_id;

	if (dimensions_mask & PGWS_DIMENSIONS_PARALLEL_LEADER_PID)
		dimensions->parallel_leader_pid = (lockGroupLeader ?
										   lockGroupLeader->pid :
										   0);

	if (dimensions_mask & PGWS_DIMENSIONS_IS_REGULAR_BE)
		dimensions->is_regular_backend = is_regular_backend;

	/* Look into BackendStatus only if necessary */
	if (check_bestatus_dimensions(dimensions_mask))
	{
#if PG_VERSION_NUM >= 170000
		PgBackendStatus	*bestatus = pgstat_get_beentry_by_proc_number(GetNumberFromPGProc(proc));
#else
		PgBackendStatus	*bestatus = get_beentry_by_procpid(proc->pid);
#endif
		/* Copy everything we need from BackendStatus */
		if (bestatus)
		{
			if (dimensions_mask & PGWS_DIMENSIONS_BE_TYPE)
				dimensions->backend_type = bestatus->st_backendType;

			if (dimensions_mask & PGWS_DIMENSIONS_BE_STATE)
				dimensions->backend_state = bestatus->st_state;

			if (dimensions_mask & PGWS_DIMENSIONS_BE_START_TIME)
				dimensions->proc_start = bestatus->st_proc_start_timestamp;

			if (dimensions_mask & PGWS_DIMENSIONS_CLIENT_ADDR)
				dimensions->client_addr = bestatus->st_clientaddr;

			if (dimensions_mask & PGWS_DIMENSIONS_CLIENT_HOSTNAME)
				strcpy(dimensions->client_hostname, bestatus->st_clienthostname);

			if (dimensions_mask & PGWS_DIMENSIONS_APPNAME)
				strcpy(dimensions->appname, bestatus->st_appname);
		}
	}
}

static void
copy_dimensions (SamplingDimensions *dst, SamplingDimensions *src,
				 int dst_dimensions_mask)
{
	if (dst_dimensions_mask & PGWS_DIMENSIONS_PID)
		dst->pid = src->pid;

	if (dst_dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT_TYPE ||
		dst_dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT)
		dst->wait_event_info = src->wait_event_info;

	if (dst_dimensions_mask & PGWD_DIMENSIONS_QUERY_ID)
		dst->queryId = src->queryId;

	if (dst_dimensions_mask & PGWD_DIMENSIONS_QUERY_ID)
		dst->role_id = src->role_id;

	if (dst_dimensions_mask & PGWS_DIMENSIONS_DB_ID)
		dst->database_id = src->database_id;

	if (dst_dimensions_mask & PGWS_DIMENSIONS_PARALLEL_LEADER_PID)
		dst->parallel_leader_pid = src->parallel_leader_pid;

	if (dst_dimensions_mask & PGWS_DIMENSIONS_IS_REGULAR_BE)
		dst->is_regular_backend = src->is_regular_backend;

	if (dst_dimensions_mask & PGWS_DIMENSIONS_BE_TYPE)
		dst->backend_type = src->backend_type;

	if (dst_dimensions_mask & PGWS_DIMENSIONS_BE_STATE)
		dst->backend_state = src->backend_state;

	if (dst_dimensions_mask & PGWS_DIMENSIONS_BE_START_TIME)
		dst->proc_start = src->proc_start;

	if (dst_dimensions_mask & PGWS_DIMENSIONS_CLIENT_ADDR)
		dst->client_addr = src->client_addr;

	if (dst_dimensions_mask & PGWS_DIMENSIONS_CLIENT_HOSTNAME)
		strcpy(dst->client_hostname, src->client_hostname);

	if (dst_dimensions_mask & PGWS_DIMENSIONS_APPNAME)
		strcpy(dst->appname, src->appname);
}

int
get_serialized_size(int dimensions_mask, bool need_last_field)
{
	int					serialized_size = 0;
	SamplingDimensions	dimensions = {0}; /* Used only for sizeof */

	if (dimensions_mask & PGWS_DIMENSIONS_PID)
		serialized_size += sizeof(dimensions.pid);
	if (dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT_TYPE ||
		dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT)
		serialized_size += sizeof(dimensions.wait_event_info);
	if (dimensions_mask & PGWD_DIMENSIONS_QUERY_ID)
		serialized_size += sizeof(dimensions.queryId);
	if (dimensions_mask & PGWS_DIMENSIONS_ROLE_ID)
		serialized_size += sizeof(dimensions.role_id);
	if (dimensions_mask & PGWS_DIMENSIONS_DB_ID)
		serialized_size += sizeof(dimensions.database_id);
	if (dimensions_mask & PGWS_DIMENSIONS_PARALLEL_LEADER_PID)
		serialized_size += sizeof(dimensions.parallel_leader_pid);
	if (dimensions_mask & PGWS_DIMENSIONS_IS_REGULAR_BE)
		serialized_size += sizeof(dimensions.is_regular_backend);
	if (dimensions_mask & PGWS_DIMENSIONS_BE_TYPE)
		serialized_size += sizeof(dimensions.backend_type);
	if (dimensions_mask & PGWS_DIMENSIONS_BE_STATE)
		serialized_size += sizeof(dimensions.backend_state);
	if (dimensions_mask & PGWS_DIMENSIONS_BE_START_TIME)
		serialized_size += sizeof(dimensions.proc_start);
	if (dimensions_mask & PGWS_DIMENSIONS_CLIENT_ADDR)
		serialized_size += sizeof(dimensions.client_addr);
	if (dimensions_mask & PGWS_DIMENSIONS_CLIENT_HOSTNAME)
		serialized_size += sizeof(dimensions.client_hostname);
	if (dimensions_mask & PGWS_DIMENSIONS_APPNAME)
		serialized_size += sizeof(dimensions.appname);
	/* timestamp of history and count of profile are both 8 bytes */
	if (need_last_field)
		serialized_size += sizeof(uint64);
	return serialized_size;
}

static void
serialize_item(SamplingDimensions dimensions, int dimensions_mask,
			   char **serialized_item, char **serialized_key, int *serialized_size,
			   TimestampTz ts, uint64 count, bool is_history)
{
	char	 dummy_array[sizeof(SamplingDimensions) + sizeof(uint64) + 1];

	memset(dummy_array, 0, sizeof(dummy_array));

	if (dimensions_mask & PGWS_DIMENSIONS_PID)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.pid,
			   sizeof(dimensions.pid));
		*serialized_size += sizeof(dimensions.pid);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT_TYPE ||
		dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.wait_event_info,
			   sizeof(dimensions.wait_event_info));
		*serialized_size += sizeof(dimensions.wait_event_info);
	}

	if (dimensions_mask & PGWD_DIMENSIONS_QUERY_ID)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.queryId,
			   sizeof(dimensions.queryId));
		*serialized_size += sizeof(dimensions.queryId);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_ROLE_ID)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.role_id,
			   sizeof(dimensions.role_id));
		*serialized_size += sizeof(dimensions.role_id);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_DB_ID)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.database_id,
			   sizeof(dimensions.database_id));
		*serialized_size += sizeof(dimensions.database_id);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_PARALLEL_LEADER_PID)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.parallel_leader_pid,
			   sizeof(dimensions.parallel_leader_pid));
		*serialized_size += sizeof(dimensions.parallel_leader_pid);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_IS_REGULAR_BE)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.is_regular_backend,
			   sizeof(dimensions.is_regular_backend));
		*serialized_size += sizeof(dimensions.is_regular_backend);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_BE_TYPE)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.backend_type,
			   sizeof(dimensions.backend_type));
		*serialized_size += sizeof(dimensions.backend_type);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_BE_STATE)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.backend_state,
			   sizeof(dimensions.backend_state));
		*serialized_size += sizeof(dimensions.backend_state);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_BE_START_TIME)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.proc_start,
			   sizeof(dimensions.proc_start));
		*serialized_size += sizeof(dimensions.proc_start);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_CLIENT_ADDR)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.client_addr,
			   sizeof(dimensions.client_addr));
		*serialized_size += sizeof(dimensions.client_addr);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_CLIENT_HOSTNAME)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.client_hostname,
			   sizeof(dimensions.client_hostname));
		*serialized_size += sizeof(dimensions.client_hostname);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_APPNAME)
	{
		memcpy(dummy_array + *serialized_size, &dimensions.appname,
			   sizeof(dimensions.appname));
		*serialized_size += sizeof(dimensions.appname);
	}

	/* copy all the fields without ts/count */
	*serialized_key = palloc0(*serialized_size);
	memcpy(*serialized_key, dummy_array, *serialized_size);

	if (is_history)
	{
		memcpy(dummy_array + *serialized_size, &ts,
			   sizeof(TimestampTz));
		*serialized_size += sizeof(TimestampTz);
	}
	else
	{
		memcpy(dummy_array + *serialized_size, &count,
			   sizeof(uint64));
		*serialized_size += sizeof(uint64);
	}

	/* copy everything */
	*serialized_item = palloc0(*serialized_size);
	memcpy(*serialized_item, dummy_array, *serialized_size);
}

void
deserialize_item(SamplingDimensions *dimensions, char *serialized_item,
				 int dimensions_mask, TimestampTz *ts, uint64 *count)
{
	int 				idx = 0;

	memset(dimensions, 0, sizeof(SamplingDimensions));

	if (dimensions_mask & PGWS_DIMENSIONS_PID)
	{
		memcpy(&dimensions->pid, serialized_item + idx,
			   sizeof(dimensions->pid));
		idx += sizeof(dimensions->pid);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT_TYPE ||
		dimensions_mask & PGWS_DIMENSIONS_WAIT_EVENT)
	{
		memcpy(&dimensions->wait_event_info, serialized_item + idx,
			   sizeof(dimensions->wait_event_info));
		idx += sizeof(dimensions->wait_event_info);
	}

	if (dimensions_mask & PGWD_DIMENSIONS_QUERY_ID)
	{
		memcpy(&dimensions->queryId, serialized_item + idx,
			   sizeof(dimensions->queryId));
		idx += sizeof(dimensions->queryId);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_ROLE_ID)
	{
		memcpy(&dimensions->role_id, serialized_item + idx,
			   sizeof(dimensions->role_id));
		idx += sizeof(dimensions->role_id);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_DB_ID)
	{
		memcpy(&dimensions->database_id, serialized_item + idx,
			   sizeof(dimensions->database_id));
		idx += sizeof(dimensions->database_id);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_PARALLEL_LEADER_PID)
	{
		memcpy(&dimensions->parallel_leader_pid, serialized_item + idx,
			   sizeof(dimensions->parallel_leader_pid));
		idx += sizeof(dimensions->parallel_leader_pid);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_IS_REGULAR_BE)
	{
		memcpy(&dimensions->is_regular_backend, serialized_item + idx,
			   sizeof(dimensions->is_regular_backend));
		idx += sizeof(dimensions->is_regular_backend);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_BE_TYPE)
	{
		memcpy(&dimensions->backend_type, serialized_item + idx,
			   sizeof(dimensions->backend_type));
		idx += sizeof(dimensions->backend_type);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_BE_STATE)
	{
		memcpy(&dimensions->backend_state, serialized_item + idx,
			   sizeof(dimensions->backend_state));
		idx += sizeof(dimensions->backend_state);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_BE_START_TIME)
	{
		memcpy(&dimensions->proc_start, serialized_item + idx,
			   sizeof(dimensions->proc_start));
		idx += sizeof(dimensions->proc_start);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_CLIENT_ADDR)
	{
		memcpy(&dimensions->client_addr, serialized_item + idx,
			   sizeof(dimensions->client_addr));
		idx += sizeof(dimensions->client_addr);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_CLIENT_HOSTNAME)
	{
		memcpy(&dimensions->client_hostname, serialized_item + idx,
			   sizeof(dimensions->client_hostname));
		idx += sizeof(dimensions->client_hostname);
	}

	if (dimensions_mask & PGWS_DIMENSIONS_APPNAME)
	{
		memcpy(&dimensions->appname, serialized_item + idx,
			   sizeof(dimensions->appname));
		idx += sizeof(dimensions->appname);
	}

	if (ts)
	{
		memcpy(ts, serialized_item + idx,
			   sizeof(TimestampTz));
		idx += sizeof(TimestampTz);
	}

	if (count)
	{
		memcpy(count, serialized_item + idx,
			   sizeof(uint64));
		idx += sizeof(uint64);
	}
}

/*
 * Read current waits from backends and write them to history array
 * and/or profile hash.
 */
static void
probe_waits(History *observations, HTAB *profile_hash,
			bool write_history, bool write_profile, bool profile_pid)
{
	int			i,
				newSize;
	TimestampTz ts = GetCurrentTimestamp();

	/* Realloc waits history if needed */
	newSize = pgws_historySize;
	if (observations->count != newSize)
		realloc_history(observations, newSize);

	/* Iterate PGPROCs under shared lock */
	LWLockAcquire(ProcArrayLock, LW_SHARED);
	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		PGPROC	   *proc = &ProcGlobal->allProcs[i];
		int 		pid;
		uint32		wait_event_info;
		SamplingDimensions common_dimensions,
						   history_dimensions,
						   profile_dimensions;
		int			dimensions_mask_common = saved_history_dimensions |
											 saved_profile_dimensions;

		/* Check if we need to sample this process */
		if (!pgws_should_sample_proc(proc, &pid, &wait_event_info))
			continue;

		/*
		 * We zero dimensions with memset to avoid doing it field-by-field
		 */
		memset(&history_dimensions, 0, sizeof(SamplingDimensions));
		memset(&profile_dimensions, 0, sizeof(SamplingDimensions));
		memset(&common_dimensions, 0, sizeof(SamplingDimensions));

		fill_dimensions(&common_dimensions, proc, pid, wait_event_info,
						pgws_proc_queryids[i], dimensions_mask_common);

		copy_dimensions(&history_dimensions,
						&common_dimensions,
						saved_history_dimensions);
		copy_dimensions(&profile_dimensions,
						&common_dimensions,
						saved_profile_dimensions);

		/* Write to the history if needed */
		if (write_history)
		{
			char		*serialized_key,
						*serialized_item,
						*observation;
			int			 serialized_size = 0;

			observation = get_next_observation(observations);
			serialize_item(history_dimensions, saved_history_dimensions,
						   &serialized_item, &serialized_key, &serialized_size,
						   ts, (uint64) 0, true);
			memcpy(observation, serialized_item, serialized_size);
		}

		/* Write to the profile if needed */
		if (write_profile)
		{
			bool		 found;
			int			 serialized_size = 0;
			uint64		 count = 1;
			char		*serialized_key,
						*serialized_item,
						*stored_item;

			if (!profile_pid)
				profile_dimensions.pid = 0;

			serialize_item(profile_dimensions, saved_profile_dimensions,
						   &serialized_item, &serialized_key, &serialized_size,
						   (TimestampTz) 0, count, false);

			stored_item = (char *) hash_search(profile_hash, serialized_key,
											   HASH_ENTER, &found);

			if (found)
			{
				memcpy(&count, (stored_item + serialized_size - sizeof(uint64)),
					   sizeof(uint64));
				count++;
				memcpy((stored_item + serialized_size - sizeof(uint64)), &count,
					   sizeof(uint64));
			}
			else
				memcpy(stored_item, serialized_item, serialized_size);
		}
	}
	LWLockRelease(ProcArrayLock);
#if PG_VERSION_NUM >= 140000
	pgstat_clear_backend_activity_snapshot();
#else
	pgstat_clear_snapshot();
#endif
}

/*
 * Send waits history to shared memory queue.
 */
static void
send_history(History *observations, shm_mq_handle *mqh)
{
	int			serialized_size = get_serialized_size(saved_history_dimensions, true);
	Size		count,
				i;
	shm_mq_result mq_result;

	if (observations->wraparound)
		count = observations->count;
	else
		count = observations->index;

	/* Send array size first since receive_array expects this */
	mq_result = shm_mq_send_compat(mqh, sizeof(count), &count, false, true);
	if (mq_result == SHM_MQ_DETACHED)
	{
		ereport(WARNING,
				(errmsg("pg_wait_sampling collector: "
						"receiver of message queue has been detached")));
		return;
	}
	/* Send saved_dimensions next */
	mq_result = shm_mq_send_compat(mqh, sizeof(saved_history_dimensions), &saved_history_dimensions, false, true);
	if (mq_result == SHM_MQ_DETACHED)
	{
		ereport(WARNING,
				(errmsg("pg_wait_sampling collector: "
						"receiver of message queue has been detached")));
		return;
	}
	for (i = 0; i < count; i++)
	{
		mq_result = shm_mq_send_compat(mqh,
									   serialized_size,
									   (observations->serialized_items + i * serialized_size),
									   false,
									   true);
		if (mq_result == SHM_MQ_DETACHED)
		{
			ereport(WARNING,
					(errmsg("pg_wait_sampling collector: "
							"receiver of message queue has been detached")));
			return;
		}
	}
}

/*
 * Send profile to shared memory queue.
 */
static void
send_profile(HTAB *profile_hash, shm_mq_handle *mqh)
{
	HASH_SEQ_STATUS scan_status;
	char	   *serialized_item;
	int			serialized_size = get_serialized_size(saved_profile_dimensions, true);
	Size		count = hash_get_num_entries(profile_hash);
	shm_mq_result mq_result;

	/* Send array size first since receive_array expects this */
	mq_result = shm_mq_send_compat(mqh, sizeof(count), &count, false, true);
	if (mq_result == SHM_MQ_DETACHED)
	{
		ereport(WARNING,
				(errmsg("pg_wait_sampling collector: "
						"receiver of message queue has been detached")));
		return;
	}
	/* Send saved_dimensions next */
	mq_result = shm_mq_send_compat(mqh, sizeof(saved_profile_dimensions), &saved_profile_dimensions, false, true);
	if (mq_result == SHM_MQ_DETACHED)
	{
		ereport(WARNING,
				(errmsg("pg_wait_sampling collector: "
						"receiver of message queue has been detached")));
		return;
	}
	hash_seq_init(&scan_status, profile_hash);
	while ((serialized_item = (char *) hash_seq_search(&scan_status)) != NULL)
	{
		mq_result = shm_mq_send_compat(mqh, serialized_size, serialized_item, false,
									   true);
		if (mq_result == SHM_MQ_DETACHED)
		{
			hash_seq_term(&scan_status);
			ereport(WARNING,
					(errmsg("pg_wait_sampling collector: "
							"receiver of message queue has been detached")));
			return;
		}
	}
}

/*
 * Make hash table for wait profile.
 */
static HTAB *
make_profile_hash()
{
	HASHCTL		hash_ctl;

	saved_profile_dimensions = pgws_profile_dimensions;

	/*
	 * Since adding additional dimensions we use SamplingDimensions as
	 * hashtable key. This is fine for cases when some fields are 0 since
	 * it doesn't impede our ability to search the hash table for entries
	 */
	hash_ctl.keysize = get_serialized_size(saved_profile_dimensions, false);
	/* entry includes SamplingDimensions and ts/count */
	hash_ctl.entrysize = get_serialized_size(saved_profile_dimensions, true);

	return hash_create("Waits profile hash", 1024, &hash_ctl,
					   HASH_ELEM | HASH_BLOBS);
}

/*
 * Delta between two timestamps in milliseconds.
 */
static int64
millisecs_diff(TimestampTz tz1, TimestampTz tz2)
{
	long		secs;
	int			microsecs;

	TimestampDifference(tz1, tz2, &secs, &microsecs);

	return secs * 1000 + microsecs / 1000;

}

/*
 * Main routine of wait history collector.
 */
void
pgws_collector_main(Datum main_arg)
{
	HTAB	   *profile_hash = NULL;
	History		observations;
	MemoryContext old_context,
				collector_context;
	TimestampTz current_ts,
				history_ts,
				profile_ts;

	/*
	 * Establish signal handlers.
	 *
	 * We want to respond to the ProcSignal notifications.  This is done in
	 * the upstream provided procsignal_sigusr1_handler, which is
	 * automatically used if a bgworker connects to a database.  But since our
	 * worker doesn't connect to any database even though it calls
	 * InitPostgres, which will still initializze a new backend and thus
	 * partitipate to the ProcSignal infrastructure.
	 */
	pqsignal(SIGTERM, handle_sigterm);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();
	InitPostgresCompat(NULL, InvalidOid, NULL, InvalidOid, 0, NULL);
	SetProcessingMode(NormalProcessing);

	/* Make pg_wait_sampling recognisable in pg_stat_activity */
	pgstat_report_appname("pg_wait_sampling collector");

	profile_hash = make_profile_hash();
	pgws_collector_hdr->latch = &MyProc->procLatch;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_wait_sampling collector");
	collector_context = AllocSetContextCreate(TopMemoryContext,
											  "pg_wait_sampling context", ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(collector_context);
	alloc_history(&observations, pgws_historySize);
	MemoryContextSwitchTo(old_context);

	ereport(LOG, (errmsg("pg_wait_sampling collector started")));

	/* Start counting time for history and profile samples */
	profile_ts = history_ts = GetCurrentTimestamp();

	while (1)
	{
		int			rc;
		shm_mq_handle *mqh;
		int64		history_diff,
					profile_diff;
		bool		write_history,
					write_profile;

		/* We need an explicit call for at least ProcSignal notifications. */
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Calculate time to next sample for history or profile */
		current_ts = GetCurrentTimestamp();

		history_diff = millisecs_diff(history_ts, current_ts);
		profile_diff = millisecs_diff(profile_ts, current_ts);

		write_history = (history_diff >= (int64) pgws_historyPeriod);
		write_profile = (profile_diff >= (int64) pgws_profilePeriod);

		if (write_history || write_profile)
		{
			probe_waits(&observations, profile_hash,
						write_history, write_profile, pgws_profilePid);

			if (write_history)
			{
				history_ts = current_ts;
				history_diff = 0;
			}

			if (write_profile)
			{
				profile_ts = current_ts;
				profile_diff = 0;
			}
		}

		/* Shutdown if requested */
		if (shutdown_requested)
			break;

		/*
		 * Wait until next sample time or request to do something through
		 * shared memory.
		 */
		rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   Min(pgws_historyPeriod - (int) history_diff,
						   pgws_historyPeriod - (int) profile_diff), PG_WAIT_EXTENSION);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		ResetLatch(&MyProc->procLatch);

		/* Handle request if any */
		if (pgws_collector_hdr->request != NO_REQUEST)
		{
			LOCKTAG		tag;
			SHMRequest	request;

			pgws_init_lock_tag(&tag, PGWS_COLLECTOR_LOCK);

			LockAcquire(&tag, ExclusiveLock, false, false);
			request = pgws_collector_hdr->request;
			pgws_collector_hdr->request = NO_REQUEST;

			if (request == HISTORY_REQUEST || request == PROFILE_REQUEST)
			{
				shm_mq_result mq_result;

				/* Send history or profile */
				shm_mq_set_sender(pgws_collector_mq, MyProc);
				mqh = shm_mq_attach(pgws_collector_mq, NULL, NULL);
				mq_result = shm_mq_wait_for_attach(mqh);
				switch (mq_result)
				{
					case SHM_MQ_SUCCESS:
						switch (request)
						{
							case HISTORY_REQUEST:
								send_history(&observations, mqh);
								break;
							case PROFILE_REQUEST:
								send_profile(profile_hash, mqh);
								break;
							default:
								Assert(false);
						}
						break;
					case SHM_MQ_DETACHED:
						ereport(WARNING,
								(errmsg("pg_wait_sampling collector: "
										"receiver of message queue have been "
										"detached")));
						break;
					default:
						Assert(false);
				}
				shm_mq_detach(mqh);
			}
			else if (request == HISTORY_RESET)
			{
				/* Reset history */
				pfree(observations.items);
				alloc_history(&observations, pgws_historySize);
			}
			else if (request == PROFILE_RESET)
			{
				/* Reset profile hash */
				hash_destroy(profile_hash);
				profile_hash = make_profile_hash();
			}
			LockRelease(&tag, ExclusiveLock, false);
		}
	}

	MemoryContextReset(collector_context);

	ereport(LOG, (errmsg("pg_wait_sampling collector shutting down")));
	proc_exit(0);
}
