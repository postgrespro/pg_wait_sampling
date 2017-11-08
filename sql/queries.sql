CREATE EXTENSION pg_wait_sampling;

SELECT SUM(0) FROM pg_wait_sampling_current;
SELECT SUM(0) FROM pg_wait_sampling_history;
SELECT SUM(0) FROM pg_wait_sampling_profile;

SELECT pg_wait_sampling_reset_profile();

DROP EXTENSION pg_wait_sampling;
