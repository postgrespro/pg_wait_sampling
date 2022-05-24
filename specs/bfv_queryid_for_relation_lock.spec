setup {
	CREATE EXTENSION pg_wait_sampling;
	CREATE EXTENSION pg_stat_statements;
	SELECT pg_stat_statements_reset();
	SELECT pg_wait_sampling_reset_profile();
}

teardown {
	DROP EXTENSION pg_stat_statements;
	DROP EXTENSION pg_wait_sampling;
}

session "s1"
	step "s1_acquire_lock_on_relation"	{
		BEGIN;
		LOCK pg_class IN ACCESS EXCLUSIVE MODE;
	}
	step "s1_wait_one_sec" {
		SELECT pg_sleep(1);
	}
	step "s1_release_relation_lock" {
		ROLLBACK;
	}

session "s2"
	step "s2_wait_on_relation_lock" {
		BEGIN;
		LOCK pg_class IN ACCESS SHARE MODE;
		ROLLBACK;
	}
	# FIXME: the profile have to expose not NULL query for wait on relation lock
	# After fix replace LEFT JOIN to INNER one
	step "s2_expose_wait_on_lock_in_profile" {
		SELECT query
		FROM pg_wait_sampling_profile pgws
		LEFT JOIN pg_stat_statements pgss using (queryid)
		WHERE pid = pg_backend_pid()
		  AND event_type = 'Lock' AND event = 'relation'
		  AND count between 90 and 110;
	}

permutation "s1_acquire_lock_on_relation" "s2_wait_on_relation_lock" "s1_wait_one_sec" "s1_release_relation_lock" "s2_expose_wait_on_lock_in_profile"
