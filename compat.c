#include "postgres.h"
#include "access/tupdesc.h"

#include "pg_wait_sampling.h"

inline void
shm_mq_detach_compat(shm_mq_handle *mqh, shm_mq *mq)
{
#if PG_VERSION_NUM >= 100000
	shm_mq_detach(mqh);
#else
	shm_mq_detach(mq);
#endif
}

inline shm_mq_result
shm_mq_send_compat(shm_mq_handle *mqh, Size nbytes, const void *data,
				   bool nowait, bool force_flush)
{
#if PG_VERSION_NUM >= 150000
	return shm_mq_send(mqh, nbytes, data, nowait, force_flush);
#else
	return shm_mq_send(mqh, nbytes, data, nowait);
#endif
}

inline TupleDesc
CreateTemplateTupleDescCompat(int nattrs, bool hasoid)
{
#if PG_VERSION_NUM >= 120000
	return CreateTemplateTupleDesc(nattrs);
#else
	return CreateTemplateTupleDesc(nattrs, hasoid);
#endif
}
