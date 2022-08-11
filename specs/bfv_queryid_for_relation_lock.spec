setup {
	create extension pg_wait_sampling;
	create extension pg_stat_statements;
	select pg_stat_statements_reset();
	select pg_wait_sampling_reset_profile();

	create function waiting_on_relation_lock_query(_pid int) returns text
	language plpgsql
	as $function$
	declare
		i int = 0;
		wait_count int;
		query_text text;
		IDLE_INTERVAL constant int = 1;
		DEADLINE constant int = 10;
	begin
		loop
			i = i + 1;

			select count, query
			into wait_count, query_text
			from pg_wait_sampling_profile pgws
			left join pg_stat_statements pgss using (queryid)
			where pid = _pid
			  and event_type = 'Lock' AND event = 'relation';

			exit when wait_count > 0;
			if i > DEADLINE / IDLE_INTERVAL then
				raise 'timed out';
			end if;

			perform pg_sleep(IDLE_INTERVAL);
		end loop;
		return query_text;
	end;
	$function$;
}

teardown {
	drop function waiting_on_relation_lock_query(int);
	drop extension pg_stat_statements;
	drop extension pg_wait_sampling;
}

session "s1"
	step "s1_acquire_lock_on_relation"	{
		begin;
		lock pg_class in access exclusive mode;
	}
	# FIXME: the profile have to expose not NULL query for wait on relation lock
	step "s1_expose_query_from_profile" {
		select waiting_on_relation_lock_query(pid)
		from pg_stat_activity
		where backend_type = 'client backend'
		  and wait_event_type = 'Lock';
	}
	step "s1_release_relation_lock" {
		rollback;
	}

session "s2"
	step "s2_wait_on_relation_lock" {
		begin;
		lock pg_class in access share mode;
		rollback;
	}

permutation "s1_acquire_lock_on_relation" "s2_wait_on_relation_lock" "s1_expose_query_from_profile" "s1_release_relation_lock"
