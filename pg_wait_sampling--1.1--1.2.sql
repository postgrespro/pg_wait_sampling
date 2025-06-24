/* contrib/pg_wait_sampling/pg_wait_sampling--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_wait_sampling UPDATE TO 1.2" to load this file. \quit

DROP FUNCTION pg_wait_sampling_get_current (
	pid int4,
	OUT pid int4,
	OUT event_type text,
	OUT event text
) CASCADE;

DROP FUNCTION pg_wait_sampling_get_history (
	OUT pid int4,
	OUT ts timestamptz,
	OUT event_type text,
	OUT event text
) CASCADE;

DROP FUNCTION pg_wait_sampling_get_profile (
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT count bigint
) CASCADE;

CREATE FUNCTION pg_wait_sampling_get_current (
	pid int4,
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT role_id int8,
	OUT database_id int8,
	OUT parallel_leader_pid int4,
	OUT is_regular_backend bool,
	OUT backend_type text,
	OUT backend_state text,
	OUT proc_start timestamptz,
	OUT client_addr text,
	OUT client_hostname text,
	OUT appname text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE CALLED ON NULL INPUT;

CREATE VIEW pg_wait_sampling_current AS
	SELECT * FROM pg_wait_sampling_get_current(NULL::integer);

GRANT SELECT ON pg_wait_sampling_current TO PUBLIC;

CREATE FUNCTION pg_wait_sampling_get_history (
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT role_id int8,
	OUT database_id int8,
	OUT parallel_leader_pid int4,
	OUT is_regular_backend bool,
	OUT backend_type text,
	OUT backend_state text,
	OUT proc_start timestamptz,
	OUT client_addr text,
	OUT client_hostname text,
	OUT appname text,
	OUT ts timestamptz
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE VIEW pg_wait_sampling_history AS
	SELECT * FROM pg_wait_sampling_get_history();

GRANT SELECT ON pg_wait_sampling_history TO PUBLIC;

CREATE FUNCTION pg_wait_sampling_get_profile (
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT role_id int8,
	OUT database_id int8,
	OUT parallel_leader_pid int4,
	OUT is_regular_backend bool,
	OUT backend_type text,
	OUT backend_state text,
	OUT proc_start timestamptz,
	OUT client_addr text,
	OUT client_hostname text,
	OUT appname text,
	OUT count int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE VIEW pg_wait_sampling_profile AS
	SELECT * FROM pg_wait_sampling_get_profile();

GRANT SELECT ON pg_wait_sampling_profile TO PUBLIC;

CREATE FUNCTION pg_wait_sampling_reset_history()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_wait_sampling_reset_history() FROM PUBLIC;
