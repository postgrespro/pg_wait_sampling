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

#include "miscadmin.h"
#include "pg_wait_sampling.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shm_mq.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"
#if PG_VERSION_NUM < 140000
#include "pgstat.h"
#else
#include "utils/wait_event.h"
#endif

static inline shm_mq_result
shm_mq_send_compat(shm_mq_handle *mqh, Size nbytes, const void *data,
				   bool nowait, bool force_flush)
{
#if PG_VERSION_NUM >= 150000
	return shm_mq_send(mqh, nbytes, data, nowait, force_flush);
#else
	return shm_mq_send(mqh, nbytes, data, nowait);
#endif
}

#if PG_VERSION_NUM < 170000
#define INIT_PG_LOAD_SESSION_LIBS		0x0001
#define INIT_PG_OVERRIDE_ALLOW_CONNS	0x0002
#endif

static inline void
InitPostgresCompat(const char *in_dbname, Oid dboid,
				   const char *username, Oid useroid,
				   bits32 flags,
				   char *out_dbname)
{
#if PG_VERSION_NUM >= 170000
	InitPostgres(in_dbname, dboid, username, useroid, flags, out_dbname);
#elif PG_VERSION_NUM >= 150000
	InitPostgres(in_dbname, dboid, username, useroid,
				 flags & INIT_PG_LOAD_SESSION_LIBS,
				 flags & INIT_PG_OVERRIDE_ALLOW_CONNS, out_dbname);
#else
	InitPostgres(in_dbname, dboid, username, useroid, out_dbname,
				 flags & INIT_PG_OVERRIDE_ALLOW_CONNS);
#endif
}

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
	observations->items = (HistoryItem *) palloc0(sizeof(HistoryItem) * count);
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
	HistoryItem	   *newitems;
	int				copyCount,
					i,
					j;

	/* Allocate new array for history */
	newitems = (HistoryItem *) palloc0(sizeof(HistoryItem) * count);

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
		memcpy(&newitems[i], &observations->items[j], sizeof(HistoryItem));
		i++;
		j++;
	}

	/* Switch to new history array */
	pfree(observations->items);
	observations->items = newitems;
	observations->index = copyCount;
	observations->count = count;
	observations->wraparound = false;
}

/*
 * Get next item of history with rotation.
 */
static HistoryItem *
get_next_observation(History *observations)
{
	HistoryItem *result;

	/* Check for wraparound */
	if (observations->index >= observations->count)
	{
		observations->index = 0;
		observations->wraparound = true;
	}
	result = &observations->items[observations->index];
	observations->index++;
	return result;
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
	TimestampTz	ts = GetCurrentTimestamp();

	/* Realloc waits history if needed */
	newSize = pgws_historySize;
	if (observations->count != newSize)
		realloc_history(observations, newSize);

	/* Iterate PGPROCs under shared lock */
	LWLockAcquire(ProcArrayLock, LW_SHARED);
	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		HistoryItem		item,
					   *observation;
		PGPROC		   *proc = &ProcGlobal->allProcs[i];

		if (!pgws_should_sample_proc(proc, &item.pid, &item.wait_event_info))
			continue;

		if (pgws_profileQueries)
			item.queryId = pgws_proc_queryids[i];
		else
			item.queryId = 0;

		item.ts = ts;

		/* Write to the history if needed */
		if (write_history)
		{
			observation = get_next_observation(observations);
			*observation = item;
		}

		/* Write to the profile if needed */
		if (write_profile)
		{
			ProfileItem	   *profileItem;
			bool			found;

			if (!profile_pid)
				item.pid = 0;

			profileItem = (ProfileItem *) hash_search(profile_hash, &item, HASH_ENTER, &found);
			if (found)
				profileItem->count++;
			else
				profileItem->count = 1;
		}
	}
	LWLockRelease(ProcArrayLock);
}

/*
 * Send waits history to shared memory queue.
 */
static void
send_history(History *observations, shm_mq_handle *mqh)
{
	Size	count,
			i;
	shm_mq_result	mq_result;

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
	for (i = 0; i < count; i++)
	{
		mq_result = shm_mq_send_compat(mqh,
								sizeof(HistoryItem),
								&observations->items[i],
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
	HASH_SEQ_STATUS	scan_status;
	ProfileItem	   *item;
	Size			count = hash_get_num_entries(profile_hash);
	shm_mq_result	mq_result;

	/* Send array size first since receive_array expects this */
	mq_result = shm_mq_send_compat(mqh, sizeof(count), &count, false, true);
	if (mq_result == SHM_MQ_DETACHED)
	{
		ereport(WARNING,
				(errmsg("pg_wait_sampling collector: "
						"receiver of message queue has been detached")));
		return;
	}
	hash_seq_init(&scan_status, profile_hash);
	while ((item = (ProfileItem *) hash_seq_search(&scan_status)) != NULL)
	{
		mq_result = shm_mq_send_compat(mqh, sizeof(ProfileItem), item, false,
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
	HASHCTL hash_ctl;

	/* We always include queryId in hash key */
	hash_ctl.keysize = offsetof(ProfileItem, count);
	hash_ctl.entrysize = sizeof(ProfileItem);
	return hash_create("Waits profile hash", 1024, &hash_ctl,
					   HASH_ELEM | HASH_BLOBS);
}

/*
 * Main routine of wait history collector.
 */
void
pgws_collector_main(Datum main_arg)
{
	HTAB		   *profile_hash = NULL;
	History			observations;
	TimestampTz		current_ts,
					history_ts,
					profile_ts;

	/* Establish signal handlers */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();
	InitPostgresCompat(NULL, InvalidOid, NULL, InvalidOid, 0, NULL);
	SetProcessingMode(NormalProcessing);


	pgws_collector_hdr->latch = &MyProc->procLatch;

	alloc_history(&observations, pgws_historySize);
	profile_hash = make_profile_hash();

	ereport(LOG, errmsg("pg_wait_sampling collector started"));

	/* Start counting time for history and profile samples */
	profile_ts = history_ts = GetCurrentTimestamp();

	while (1)
	{
		shm_mq_handle  *mqh;
		int64			history_diff,
						profile_diff;
		bool			write_history,
						write_profile;

		HandleMainLoopInterrupts();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Calculate time to next sample for history or profile */
		current_ts = GetCurrentTimestamp();

		history_diff = TimestampDifferenceMilliseconds(history_ts, current_ts);
		profile_diff = TimestampDifferenceMilliseconds(profile_ts, current_ts);

		write_history = (history_diff >= (int64)pgws_historyPeriod);
		write_profile = (profile_diff >= (int64)pgws_profilePeriod);

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

		/*
		 * Wait for sample time or until request to do something through
		 * shared memory.
		 */
		WaitLatch(&MyProc->procLatch,
				  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  Min(pgws_historyPeriod - (int)history_diff,
					  pgws_profilePeriod - (int)profile_diff),
				  PG_WAIT_EXTENSION);

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
				shm_mq_result	mq_result;

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
			else if (request == PROFILE_RESET)
			{
				/* Reset profile hash */
				hash_destroy(profile_hash);
				profile_hash = make_profile_hash();
			}
			LockRelease(&tag, ExclusiveLock, false);
		}
	}
}
