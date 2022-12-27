/*
 * pg_wait_sampling.h
 *		Headers for pg_wait_sampling extension.
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_wait_sampling/pg_wait_sampling.h
 */
#ifndef __PG_WAIT_SAMPLING_H__
#define __PG_WAIT_SAMPLING_H__

#include <postgres.h>

/* Check PostgreSQL version */
#if PG_VERSION_NUM < 90600
	#error "You are trying to build pg_wait_sampling with PostgreSQL version lower than 9.6.  Please, check you environment."
#endif

#include "utils/timestamp.h"

#define	PG_WAIT_SAMPLING_MAGIC		0xCA94B107

typedef struct
{
	uint32			pid;
	uint32			wait_event_info;
	pgwsQueryId		queryId;
	TimestampTz		ts;
} HistoryItem;

typedef struct
{
	Size		index;
	HistoryItem	items[FLEXIBLE_ARRAY_MEMBER];
} History;

/*
 * Hashtable key that defines the identity of a hashtable entry
 */
typedef struct
{
	int32		pid;			/* pid of observable process */
	uint32		wait_event_info;/* proc's wait information */
	pgwsQueryId	queryid;		/* query identifier */
} ProfileHashKey;

/*
 * Wait statistics entry
 */
typedef struct
{
	ProfileHashKey	key;		/* hash key of entry - MUST BE FIRST */
	int64			counter;	/* cummulative counter for this entry */
	double			usage;		/* usage factor */
} ProfileHashEntry;

/* pg_wait_sampling.c */
extern pgwsQueryId	*pgws_proc_queryids;
extern HTAB 		*pgws_hash;
extern LWLock 		*pgws_hash_lock;
extern History		*pgws_history_ring;
extern LWLock		*pgws_history_lock;

/* global settings */
extern int MaxProfileEntries;
extern int HistoryBufferSize;
extern int HistoryPeriod;
extern int ProfilePeriod;
extern bool WhetherProfilePid;
extern bool WhetherProfileQueryId;

/* collector.c */
extern PGDLLEXPORT void pgws_collector_main(Datum main_arg);

#endif
