/*
 * pg_wait_sampling.h
 *		Headers for pg_wait_sampling extension.
 *
 * Copyright (c) 2015-2025, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_wait_sampling/pg_wait_sampling.h
 */
#ifndef __PG_WAIT_SAMPLING_H__
#define __PG_WAIT_SAMPLING_H__

#include "datatype/timestamp.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/shm_mq.h"

#if PG_VERSION_NUM >= 140000
#include "utils/backend_status.h"
#else
#include "pgstat.h"
#endif

#define	PG_WAIT_SAMPLING_MAGIC		0xCA94B107
#define COLLECTOR_QUEUE_SIZE		(16 * 1024)
#define HISTORY_TIME_MULTIPLIER		10
#define PGWS_QUEUE_LOCK				0
#define PGWS_COLLECTOR_LOCK			1

/* Values for sampling dimensions */
#define PGWS_DIMENSIONS_NONE				0

#define PGWS_DIMENSIONS_ROLE_ID				(1 << 0)
#define PGWS_DIMENSIONS_DB_ID				(1 << 1)
#define PGWS_DIMENSIONS_PARALLEL_LEADER_PID	(1 << 2)
#define PGWS_DIMENSIONS_IS_REGULAR_BE		(1 << 3)
#define PGWS_DIMENSIONS_BE_TYPE				(1 << 4)
#define PGWS_DIMENSIONS_BE_STATE			(1 << 5)
#define PGWS_DIMENSIONS_BE_START_TIME		(1 << 6)
#define PGWS_DIMENSIONS_CLIENT_ADDR			(1 << 7)
#define PGWS_DIMENSIONS_CLIENT_HOSTNAME		(1 << 8)
#define PGWS_DIMENSIONS_APPNAME				(1 << 9)

#define PGWS_DIMENSIONS_ALL					((int) ~0)
/* ^ all 1 in binary */

/*
 * Common data (sampling dimenstions) for ProfileItem and HistoryItem
 */
typedef struct
{
	/* Fields from PGPROC */
	int			 pid;
	uint32		 wait_event_info;
	uint64		 queryId;
	Oid			 role_id;
	Oid			 database_id;
	int			 parallel_leader_pid;
	bool		 is_regular_backend;
	/* Fields from BackendStatus */
	BackendType	 backend_type;
	BackendState backend_state;
	TimestampTz	 proc_start;
	SockAddr	 client_addr;
	char		 client_hostname[NAMEDATALEN];
	char		 appname[NAMEDATALEN];
} SamplingDimensions;

/*
 * Next two structures must match in fields until count/ts so make_profile_hash
 * works properly
 */
typedef struct
{
	SamplingDimensions	dimensions;
	uint64				count;
} ProfileItem;

typedef struct
{
	SamplingDimensions	dimensions;
	TimestampTz			ts;
} HistoryItem;

typedef struct
{
	bool		wraparound;
	Size		index;
	Size		count;
	HistoryItem *items;
} History;

typedef enum
{
	NO_REQUEST,
	HISTORY_REQUEST,
	PROFILE_REQUEST,
	PROFILE_RESET
} SHMRequest;

typedef struct
{
	Latch	   *latch;
	SHMRequest	request;
} CollectorShmqHeader;

/* GUC variables */
extern int	pgws_historySize;
extern int	pgws_historyPeriod;
extern int	pgws_profilePeriod;
extern bool pgws_profilePid;
extern int	pgws_profileQueries;
extern bool pgws_sampleCpu;

/* pg_wait_sampling.c */
extern CollectorShmqHeader *pgws_collector_hdr;
extern shm_mq *pgws_collector_mq;
extern uint64 *pgws_proc_queryids;
extern void pgws_init_lock_tag(LOCKTAG *tag, uint32 lock);
extern bool pgws_should_sample_proc(PGPROC *proc, int *pid_p, uint32 *wait_event_info_p);
extern int pgws_history_dimensions; /* bit mask that is derived from GUC */
extern int pgws_profile_dimensions; /* bit mask that is derived from GUC */
extern PgBackendStatus* get_beentry_by_procpid(int pid);

/* collector.c */
extern void pgws_register_wait_collector(void);
extern PGDLLEXPORT void pgws_collector_main(Datum main_arg);

#endif
