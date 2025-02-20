/*
 * compat.h
 *		Definitions for function wrappers compatible between PG versions.
 *
 * Copyright (c) 2015-2025, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_wait_sampling/compat.h
 */
#ifndef __COMPAT_H__
#define __COMPAT_H__

#include "miscadmin.h"
#include "storage/shm_mq.h"

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

#endif
