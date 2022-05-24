setup {
	CREATE EXTENSION pg_wait_sampling;
	CREATE EXTENSION pg_stat_statements;
	SELECT pg_stat_statements_reset();
	SELECT pg_wait_sampling_reset_profile();

	CREATE TABLE test(i int);
	INSERT INTO test VALUES (1);
}

teardown {
	DROP TABLE test;
	DROP EXTENSION pg_stat_statements;
	DROP EXTENSION pg_wait_sampling;
}

session "s1"
	step "s1_update_tuple" {
		BEGIN;
		UPDATE test SET i = i+1;
	}
	step "s1_wait_one_sec" {
		SELECT pg_sleep(1);
	}
	step "s1_rollback_txn" {
		ROLLBACK;
	}

session "s2"
	step "s2_try_to_concurrently_update_tuple" {
		BEGIN;
		UPDATE test SET i = i+1;
		ROLLBACK;
	}

	# For GUC profile_period = 10ms we expect 100 samples in Lock event type
	# with some little deviation, e.g., 10 samples
	step "s2_expose_wait_on_txn_in_profile" {
		SELECT query
		FROM pg_wait_sampling_profile pgws
		JOIN pg_stat_statements pgss using (queryid)
		WHERE pid = pg_backend_pid()
		  AND event_type = 'Lock' AND event = 'transactionid'
		  AND count between 90 and 110;
	}

permutation "s1_update_tuple" "s2_try_to_concurrently_update_tuple" "s1_wait_one_sec" "s1_rollback_txn" "s2_expose_wait_on_txn_in_profile"
