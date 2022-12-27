/*
 * compat.h
 *		Definitions for function wrappers compatible between PG versions.
 *
 * Copyright (c) 2015-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_wait_sampling/compat.h
 */
#ifndef __COMPAT_H__
#define __COMPAT_H__

#include "postgres.h"

#include "access/tupdesc.h"
#include "miscadmin.h"
#include "storage/latch.h"

#if PG_VERSION_NUM >= 110000
typedef uint64 pgwsQueryId;
#else
typedef uint32 pgwsQueryId;
#endif

static inline TupleDesc
CreateTemplateTupleDescCompat(int nattrs, bool hasoid)
{
#if PG_VERSION_NUM >= 120000
	return CreateTemplateTupleDesc(nattrs);
#else
	return CreateTemplateTupleDesc(nattrs, hasoid);
#endif
}

static inline void
InitPostgresCompat(const char *in_dbname, Oid dboid,
				   const char *username, Oid useroid,
				   bool load_session_libraries,
				   bool override_allow_connections,
				   char *out_dbname)
{
#if PG_VERSION_NUM >= 150000
	InitPostgres(in_dbname, dboid, username, useroid, load_session_libraries,
				 override_allow_connections, out_dbname);
#elif PG_VERSION_NUM >= 110000
	InitPostgres(in_dbname, dboid, username, useroid, out_dbname,
				 override_allow_connections);
#else
	InitPostgres(in_dbname, dboid, username, useroid, out_dbname);
#endif
}

static inline int
WaitLatchCompat(Latch *latch, int wakeEvents, long timeout,
				uint32 wait_event_info)
{
#if PG_VERSION_NUM >= 100000
	return WaitLatch(latch, wakeEvents, timeout, wait_event_info);
#else
#define PG_WAIT_EXTENSION -1
	return WaitLatch(latch, wakeEvents, timeout);
#endif
}

#if PG_VERSION_NUM < 100000
#define GetPGProcByNumber(n) (&ProcGlobal->allProcs[(n)])
#endif

#endif
