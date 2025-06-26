[![Build Status](https://app.travis-ci.com/postgrespro/pg_wait_sampling.svg?branch=master)](https://app.travis-ci.com/postgrespro/pg_wait_sampling)
[![GitHub license](https://img.shields.io/badge/license-PostgreSQL-blue.svg)](https://raw.githubusercontent.com/postgrespro/pg_wait_sampling/master/LICENSE)

`pg_wait_sampling` – sampling based statistics of wait events
=============================================================

Introduction
------------

PostgreSQL provides information about current wait event of particular
process.  However, in order to gather descriptive statistics of server
behavior users have to sample current wait events multiple times.
`pg_wait_sampling` is an extension for collecting sampling statistics of wait
events.

The module must be loaded by adding `pg_wait_sampling` to
`shared_preload_libraries` in postgresql.conf, because it requires additional
shared memory and launches a background worker.  This means that a server restart
is needed to add or remove the module.

When used with `pg_stat_statements` it is recommended to put `pg_stat_statements`
before `pg_wait_sampling` in `shared_preload_libraries` so queryIds of
utility statements are not rewritten by the former.

When `pg_wait_sampling` is enabled, it collects two kinds of statistics.

 * History of waits events.  It's implemented as in-memory ring buffer where
   samples of each process' wait events are written with given (configurable)
   period.  Therefore, for each running process user can see some number of
   recent samples depending on history size (configurable). Assuming there is
   a client who periodically reads this history and dumps it somewhere, user
   can have continuous history of wait events.
 * Waits profile.  It's implemented as in-memory hash table where samples
   are accumulated per each wait event and can be divided by process, wait event,
   query and other dimensions. This hash table can be reset by user request.
   Assuming there is a client who periodically dumps profile and resets it,
   user can have statistics of wait events over time.

In combination with `pg_stat_statements` this extension can also provide
per query statistics.

`pg_wait_sampling` launches special background worker for gathering the
statistics above.

Availability
------------

`pg_wait_sampling` is implemented as an extension and not available in default
PostgreSQL installation. It is available from
[github](https://github.com/postgrespro/pg_wait_sampling)
under the same license as
[PostgreSQL](http://www.postgresql.org/about/licence/)
and supports PostgreSQL 13+.

Installation
------------

Pre-built `pg_wait_sampling` packages are provided in official PostgreSQL
repository: https://download.postgresql.org/pub/repos/

Manual build
------------

`pg_wait_sampling` is PostgreSQL extension which requires PostgreSQL 13 or
higher. Before build and install you should ensure following:

 * PostgreSQL version is 13 or higher.
 * You have development package of PostgreSQL installed or you have built
   PostgreSQL from source.
 * Your PATH variable is configured so that `pg_config` command available, or
   PG_CONFIG variable is set.

Typical installation procedure may look like this:

    $ git clone https://github.com/postgrespro/pg_wait_sampling.git
    $ cd pg_wait_sampling
    $ make USE_PGXS=1
    $ sudo make USE_PGXS=1 install

Then add `shared_preload_libraries = pg_wait_sampling` to `postgresql.conf` and
restart the server.

To test your installation:

    $ make USE_PGXS=1 installcheck

To create the extension in the target database:

    CREATE EXTENSION pg_wait_sampling;

Compilation on Windows is not supported, since the extension uses symbols from PostgreSQL
that are not exported.

Usage
-----

`pg_wait_sampling` interacts with user by set of views and functions.

`pg_wait_sampling_current` view – information about current wait events for
all processes including background workers.

|     Column name     | Column type |         Description         |
| ------------------- | ----------- | --------------------------- |
| pid                 | int4        | Id of process               |
| event_type          | text        | Name of wait event type     |
| event               | text        | Name of wait event          |
| queryid             | int8        | Id of query                 |
| role_id             | int4        | Id of role                  |
| database_id         | int4        | Id of database              |
| parallel_leader_pid | int4        | Id of parallel query leader |
| backend_type        | text        | Name of backend type        |
| backend_state       | text        | Name of backend state       |
| proc_start          | timestamptz | Timestamp of process start  |
| client_addr         | text        | Client address              |
| client_hostname     | text        | Client hostname             |
| appname             | text        | Application name            |

`pg_wait_sampling_get_current(pid int4)` returns the same table for single given
process.

`pg_wait_sampling_history` view – history of wait events obtained by sampling into
in-memory ring buffer.

|     Column name     | Column type |         Description         |
| ------------------- | ----------- | --------------------------- |
| pid                 | int4        | Id of process               |
| event_type          | text        | Name of wait event type     |
| event               | text        | Name of wait event          |
| queryid             | int8        | Id of query                 |
| role_id             | int4        | Id of role                  |
| database_id         | int4        | Id of database              |
| parallel_leader_pid | int4        | Id of parallel query leader |
| backend_type        | text        | Name of backend type        |
| backend_state       | text        | Name of backend state       |
| proc_start          | timestamptz | Timestamp of process start  |
| client_addr         | text        | Client address              |
| client_hostname     | text        | Client hostname             |
| appname             | text        | Application name            |
| ts                  | timestamptz | Sample timestamp            |

`pg_wait_sampling_reset_history()` function resets the history.

`pg_wait_sampling_profile` view – profile of wait events obtained by sampling into
in-memory hash table.

|     Column name     | Column type |         Description         |
| ------------------- | ----------- | --------------------------- |
| pid                 | int4        | Id of process               |
| event_type          | text        | Name of wait event type     |
| event               | text        | Name of wait event          |
| queryid             | int8        | Id of query                 |
| role_id             | int4        | Id of role                  |
| database_id         | int4        | Id of database              |
| parallel_leader_pid | int4        | Id of parallel query leader |
| backend_type        | text        | Name of backend type        |
| backend_state       | text        | Name of backend state       |
| proc_start          | timestamptz | Timestamp of process start  |
| client_addr         | text        | Client address              |
| client_hostname     | text        | Client hostname             |
| appname             | text        | Application name            |
| count               | int8        | Count of samples            |

`pg_wait_sampling_reset_profile()` function resets the profile.

The work of wait event statistics collector worker is controlled by following
GUCs.

| Parameter name                      | Data type | Description                                 | Default value                                |
|-------------------------------------| --------- |---------------------------------------------|----------------------------------------------|
| pg_wait_sampling.history_size       | int4      | Size of history in-memory ring buffer       |                                         5000 |
| pg_wait_sampling.history_period     | int4      | Period for history sampling in milliseconds |                                           10 |
| pg_wait_sampling.profile_period     | int4      | Period for profile sampling in milliseconds |                                           10 |
| pg_wait_sampling.profile_pid        | bool      | Whether profile should be per pid           |                                         true |
| pg_wait_sampling.profile_queries    | enum      | Whether profile should be per query         |                                          top |
| pg_wait_sampling.sample_cpu         | bool      | Whether on CPU backends should be sampled   |                                         true |
| pg_wait_sampling.history_dimensions | text      | Additional columns in extended history view | 'pid, wait_event_type, wait_event, query_id' |
| pg_wait_sampling.profile_dimensions | text      | Additional columns in extended profile view | 'pid, wait_event_type, wait_event, query_id' |

If `pg_wait_sampling.profile_pid` is set to false, sampling profile wouldn't be
collected in per-process manner.  In this case the value of pid could would
be always zero and corresponding row contain samples among all the processes.

If `pg_wait_sampling.profile_queries` is set to `none`, `queryid` field in
views will be zero. If it is set to `top`, queryIds only of top level statements
are recorded. If it is set to `all`, queryIds of nested statements are recorded.

If `pg_wait_sampling.sample_cpu` is set to true then processes that are not
waiting on anything are also sampled. The wait event columns for such processes
will be NULL.

`pg_wait_sampling.history_dimenstions` and `pg_wait_sampling.profile_dimensions`
determine what additional columns will be sampled in `history/profile_extended`
views. Possible values are `all`, `pid`, `wait_event_type`, `wait_event`,
`query_id`, `role_id`, `database_id`, `parallel_leader_pid`, `backend_type`,
`backend_state`, `backend_start_time`, `client_addr`, `client_hostname`,
`appname` and any combination of column names.
`all` cannot be used together with any other values and must be used alone.

> [!WARNING]
> Turning on any of the following columns: `backend_type`, `backend_state`,
> `backend_start_time`, `client_addr`, `client_hostname`, `appname` will reduce
> performance compared to sampling none of those due to the need to look into
> BackendStatusTable. This is especially noticeable with PostgreSQL 13-16

Values of these GUC variables can be changed only in config file or with ALTER SYSTEM.
Then you need to reload server's configuration (such as with `pg_reload_conf` function)
for changes to take effect.

> [!WARNING]
> When using `pg_reload_conf` you also need to invoke `pg_wait_sampling_reset_history()`
> and `pg_wait_sampling_reset_profile()` for correct application of new dimensions

See
[PostgreSQL documentation](http://www.postgresql.org/docs/devel/static/monitoring-stats.html#WAIT-EVENT-TABLE)
for list of possible wait events.

Contribution
------------

Please, notice, that `pg_wait_sampling` is still under development and while
it's stable and tested, it may contain some bugs. Don't hesitate to raise
[issues at github](https://github.com/postgrespro/pg_wait_sampling/issues) with
your bug reports.

If you're lacking of some functionality in `pg_wait_sampling` and feeling power
to implement it then you're welcome to make pull requests.

Authors
-------

 * Alexander Korotkov <a.korotkov@postgrespro.ru>, Postgres Professional,
   Moscow, Russia
 * Ildus Kurbangaliev <i.kurbangaliev@gmail.com>, Postgres Professional,
   Moscow, Russia

