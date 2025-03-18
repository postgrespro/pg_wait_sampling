/* contrib/pg_wait_sampling/pg_wait_sampling--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_wait_sampling UPDATE TO 1.2" to load this file. \quit

CREATE FUNCTION pg_wait_sampling_get_current_extended (
	pid int4,
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT role_id int8,
	OUT database_id int8,
	OUT parallel_leader_pid int4,
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

CREATE VIEW pg_wait_sampling_current_extended AS
	SELECT * FROM pg_wait_sampling_get_current_extended(NULL::integer);

GRANT SELECT ON pg_wait_sampling_current TO PUBLIC;

CREATE FUNCTION pg_wait_sampling_get_history_extended (
	OUT pid int4,
	OUT ts timestamptz,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT role_id int8,
	OUT database_id int8,
	OUT parallel_leader_pid int4,
	OUT backend_type text,
	OUT backend_state text,
	OUT proc_start timestamptz,
	OUT client_addr text,
	OUT client_hostname text,
	OUT appname text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE VIEW pg_wait_sampling_history_extended AS
	SELECT * FROM pg_wait_sampling_get_history_extended();

GRANT SELECT ON pg_wait_sampling_history_extended TO PUBLIC;

CREATE FUNCTION pg_wait_sampling_get_profile_extended (
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT role_id int8,
	OUT database_id int8,
	OUT parallel_leader_pid int4,
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

CREATE VIEW pg_wait_sampling_profile_extended AS
	SELECT * FROM pg_wait_sampling_get_profile_extended();

GRANT SELECT ON pg_wait_sampling_profile_extended TO PUBLIC;
