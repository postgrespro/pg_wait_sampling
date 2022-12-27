[![Build Status](https://travis-ci.com/postgrespro/pg_wait_sampling.svg?branch=master)](https://travis-ci.com/postgrespro/pg_wait_sampling)
[![PGXN version](https://badge.fury.io/pg/pg_wait_sampling.svg)](https://badge.fury.io/pg/pg_wait_sampling)
[![GitHub license](https://img.shields.io/badge/license-PostgreSQL-blue.svg)](https://raw.githubusercontent.com/postgrespro/pg_wait_sampling/master/LICENSE)

`pg_wait_sampling` – sampling based statistics of wait events
=============================================================

Introduction
------------

PostgreSQL 9.6+ provides an information about current wait event of particular
process.  However, in order to gather descriptive statistics of server
behavior user have to sample current wait event multiple times.
`pg_wait_sampling` is an extension for collecting sampling statistics of wait
events.

The module must be loaded by adding `pg_wait_sampling` to
`shared_preload_libraries` in postgresql.conf, because it requires additional
shared memory and launches background worker.  This means that a server restart
is needed to add or remove the module.

When `pg_wait_sampling` is enabled, it collects two kinds of statistics.

 * History of waits events.  It's implemented as in-memory ring buffer where
   samples of each process wait events are written with given (configurable)
   period.  Therefore, for each running process user can see some number of
   recent samples depending on history size (configurable).  Assuming there is
   a client who periodically read this history and dump it somewhere, user
   can have continuous history.
 * Waits profile.  It's implemented as bounded in-memory hash table where counts
   of samples are accumulated per triple of process pid, wait event and query id
   (when its computing is enabled on PG server, on versions below 14 this
   requires `pg_stat_statements` extension). The least used entries are evicted
   when overflow of hash table is encountered. Hash table also can be reset by
   user request. Assuming there is a client who periodically dumps profile and
   computes differential counters from adjacent dumps, user can have statistics
   of intensivity of wait events among time.

Starting from PG14 this extension might activate computing of query id on server
side to enable per query id statistics. The older PG versions require to install
`pg_stat_statements` extension for this purpose.

`pg_wait_sampling` launches special background worker for gathering the
statistics above.

The profile statistics as well as history items are not persisted to disk so
server restart resets all already accummulated data. This is not crucial for
profile counters because we are primarily interested in differential values, not
absolute values of these counters.

Availability
------------

`pg_wait_sampling` is implemented as an extension and not available in default
PostgreSQL installation. It is available from
[github](https://github.com/postgrespro/pg_wait_sampling)
under the same license as
[PostgreSQL](http://www.postgresql.org/about/licence/)
and supports PostgreSQL 9.6+.

Installation
------------

Pre-built `pg_wait_sampling` packages are provided in official PostgreSQL
repository: https://download.postgresql.org/pub/repos/

Manual build
------------

`pg_wait_sampling` is PostgreSQL extension which requires PostgreSQL 9.6 or
higher. Before build and install you should ensure following:

 * PostgreSQL version is 9.6 or higher.
 * You have development package of PostgreSQL installed or you built
   PostgreSQL from source.
 * Your PATH variable is configured so that `pg_config` command available, or
   set PG_CONFIG variable.

Typical installation procedure may look like this:

    $ git clone https://github.com/postgrespro/pg_wait_sampling.git
    $ cd pg_wait_sampling
    $ make USE_PGXS=1
    $ sudo make USE_PGXS=1 install
    $ make USE_PGXS=1 installcheck
    $ psql DB -c "CREATE EXTENSION pg_wait_sampling;"

Compilation on Windows is not supported, since the extension uses symbols from PostgreSQL
that are not exported.

Usage
-----

`pg_wait_sampling` interacts with user by set of views and functions.

`pg_wait_sampling_current` view – information about current wait events for
all processed including background workers.

| Column name | Column type |      Description        |
| ----------- | ----------- | ----------------------- |
| pid         | int4        | Id of process           |
| event_type  | text        | Name of wait event type |
| event       | text        | Name of wait event      |
| queryid     | int8        | Id of query             |

`pg_wait_sampling_get_current(pid int4)` returns the same table for single given
process.

`pg_wait_sampling_history` view – history of wait events obtained by sampling into
in-memory ring buffer.

| Column name | Column type |      Description        |
| ----------- | ----------- | ----------------------- |
| pid         | int4        | Id of process           |
| ts          | timestamptz | Sample timestamp        |
| event_type  | text        | Name of wait event type |
| event       | text        | Name of wait event      |
| queryid     | int8        | Id of query             |

`pg_wait_sampling_profile` view – profile of wait events obtained by sampling into
in-memory hash table.

| Column name | Column type |      Description        |
| ----------- | ----------- | ----------------------- |
| pid         | int4        | Id of process           |
| event_type  | text        | Name of wait event type |
| event       | text        | Name of wait event      |
| queryid     | int8        | Id of query             |
| count       | text        | Count of samples        |

`pg_wait_sampling_reset_profile()` function resets the profile.

The work of wait event statistics collector worker is controlled by following
GUCs.

|         Parameter name               | Data type |                  Description                                                        | Default value | Change policy |
| ------------------------------------ | --------- | ----------------------------------------------------------------------------------- | ------------- | ------------- |
| pg_wait_sampling.max_profile_entries | int4      | Maximum number of entries in profile hash table                                     |          5000 |       restart |
| pg_wait_sampling.history_size        | int4      | Size of history in-memory ring buffer                                               |          5000 | 		 restart |
| pg_wait_sampling.profile_period      | int4      | Period for profile sampling in milliseconds (zero value disables profile gathering) |            10 |        reload |
| pg_wait_sampling.history_period      | int4      | Period for history sampling in milliseconds (zero value disables history gathering) |             0 |		  reload |
| pg_wait_sampling.profile_pid         | bool      | Whether profile should be per pid                                                   |          true |       restart |
| pg_wait_sampling.profile_queries     | bool      | Whether profile should be per query			                                     |          true |       restart |

If `pg_wait_sampling.profile_pid` is set to false, sampling profile wouldn't be
collected in per-process manner.  In this case the value of pid will be NULL and
corresponding rows contain samples among all the processes.

__Caution__:
When sampling per pid is enabled, all profile entries for already completed
processes are left in hash table. Therefore, it's neccessary to take into
account periodic flushing of profile to prevent recycling of 32-bit pid values
in profile hash table and as consequence possible increments to profile entries
belonging to some old processes with the same pid values as for current ones.

While `pg_wait_sampling.profile_queries` is set to false `queryid` field in
views will be NULL.

See
[PostgreSQL documentation](http://www.postgresql.org/docs/devel/static/monitoring-stats.html#WAIT-EVENT-TABLE)
for list of possible wait events.

Contribution
------------

Please, notice, that `pg_wait_sampling` is still under development and while
it's stable and tested, it may contains some bugs. Don't hesitate to raise
[issues at github](https://github.com/postgrespro/pg_wait_sampling/issues) with
your bug reports.

If you're lacking of some functionality in `pg_wait_sampling` and feeling power
to implement it then you're welcome to make pull requests.

Releases
--------

New features are developed in feature-branches and then merged into [master](https://github.com/postgrespro/pg_wait_sampling/tree/master). To make a new release:

1) Bump `PGXN` version in the `META.json`.
2) Merge [master](https://github.com/postgrespro/pg_wait_sampling/tree/master) into [stable](https://github.com/postgrespro/pg_wait_sampling/tree/stable).
3) Tag new release in the [stable](https://github.com/postgrespro/pg_wait_sampling/tree/stable) with `git tag -a v1.1.X`, where the last digit is used for indicating compatible shared library changes and bugfixes. Second digit is used to indicate extension schema change, i.e. when `ALTER EXTENSION pg_wait_sampling UPDATE;` is required.
4) Merge [stable](https://github.com/postgrespro/pg_wait_sampling/tree/stable) into [debian](https://github.com/postgrespro/pg_wait_sampling/tree/debian). This separate branch is used to independently support `Debian` packaging and @anayrat with @df7cb have an access there. 

Authors
-------

 * Alexander Korotkov <a.korotkov@postgrespro.ru>, Postgres Professional,
   Moscow, Russia
 * Ildus Kurbangaliev <i.kurbangaliev@gmail.com>, Postgres Professional,
   Moscow, Russia

