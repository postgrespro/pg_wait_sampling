CREATE EXTENSION pg_wait_sampling;

WITH t as (SELECT sum(0) FROM pg_wait_sampling_current)
	SELECT sum(0) FROM generate_series(1, 2), t;

WITH t as (SELECT sum(0) FROM pg_wait_sampling_history)
	SELECT sum(0) FROM generate_series(1, 2), t;

WITH t as (SELECT sum(0) FROM pg_wait_sampling_profile)
	SELECT sum(0) FROM generate_series(1, 2), t;

SELECT pg_wait_sampling_reset_profile();

DROP EXTENSION pg_wait_sampling;
