[![Build Status](https://app.travis-ci.com/postgrespro/pg_wait_sampling.svg?branch=master)](https://app.travis-ci.com/postgrespro/pg_wait_sampling)
[![GitHub license](https://img.shields.io/badge/license-PostgreSQL-blue.svg)](https://raw.githubusercontent.com/postgrespro/pg_wait_sampling/master/LICENSE)

# pg_wait_sampling

Sampling-based statistics of wait events.

## Introduction

PostgreSQL reports the current wait event for each backend. However, gathering descriptive statistics about server behavior requires repeatedly sampling wait events. `pg_wait_sampling` automates this process and provides sampling-based statistics for wait events.

The extension must be loaded via `shared_preload_libraries`, because it allocates shared memory and runs a background worker. Any change to the preload list requires a server restart.

When `pg_stat_statements` is used together with `pg_wait_sampling`, list `pg_stat_statements` before `pg_wait_sampling` in `shared_preload_libraries`. That order keeps utility statement query IDs intact.

Once enabled, `pg_wait_sampling` collects two types of statistics:

- **History**: an in-memory ring buffer that stores recent wait-event samples for every backend. A client can periodically read and archive this history to build a continuous timeline.
- **Profile**: an in-memory hash table that aggregates wait-event sample counts per backend, wait event, and (optionally) query when `pg_stat_statements` is available. A client can periodically dump and reset the profile to monitor wait intensity over time.

A dedicated background worker populates these statistics.

## Availability

`pg_wait_sampling` is distributed as a PostgreSQL extension under the PostgreSQL license. Source code is available on [GitHub](https://github.com/postgrespro/pg_wait_sampling), and PostgreSQL 13 or later is required.

## Installation

Pre-built packages are published in the official PostgreSQL repository: https://download.postgresql.org/pub/repos/

### Manual build

`pg_wait_sampling` requires PostgreSQL 13 or newer. Before building from source, make sure:

- A supported PostgreSQL version is installed or built from source.
- The PostgreSQL development files are available.
- `pg_config` is on `PATH`, or `PG_CONFIG` points to it.

Typical build sequence:

```sh
git clone https://github.com/postgrespro/pg_wait_sampling.git
cd pg_wait_sampling
make USE_PGXS=1
sudo make USE_PGXS=1 install
```

Add `pg_wait_sampling` to `shared_preload_libraries` in `postgresql.conf` and restart the server.

To run regression tests:

```sh
make USE_PGXS=1 installcheck
```

Create the extension in the target database:

```sql
CREATE EXTENSION pg_wait_sampling;
```

Compilation on Windows is not supported because the extension relies on PostgreSQL symbols that are undefined on that platform.

## Usage

`pg_wait_sampling` exposes views, functions, and GUC parameters.

### Views

#### `pg_wait_sampling_current`

Current wait events for all processes, including background workers.

| Column | Type | Description |
|--------|------|-------------|
| `pid` | `int4` | Process ID |
| `event_type` | `text` | Wait event type |
| `event` | `text` | Wait event name |
| `queryid` | `int8` | Query identifier |

#### `pg_wait_sampling_history`

History of wait events sampled into an in-memory ring buffer.

| Column | Type | Description |
|--------|------|-------------|
| `pid` | `int4` | Process ID |
| `ts` | `timestamptz` | Sample timestamp |
| `event_type` | `text` | Wait event type |
| `event` | `text` | Wait event name |
| `queryid` | `int8` | Query identifier |

#### `pg_wait_sampling_profile`

Aggregated wait-event samples collected in an in-memory hash table.

| Column | Type | Description |
|--------|------|-------------|
| `pid` | `int4` | Process ID |
| `event_type` | `text` | Wait event type |
| `event` | `text` | Wait event name |
| `queryid` | `int8` | Query identifier |
| `count` | `int8` | Number of samples |

### Functions

- `pg_wait_sampling_get_current(pid int4)`: returns the same columns as `pg_wait_sampling_current` for a single backend.
- `pg_wait_sampling_reset_profile()`: clears the aggregated wait profile.

### Configuration parameters

The background worker is controlled by these GUC parameters:

| Parameter | Type | Description | Default |
|-----------|------|-------------|--------:|
| `pg_wait_sampling.history_size` | `int4` | Size of the in-memory history buffer | 5000 |
| `pg_wait_sampling.history_period` | `int4` | History sampling period in milliseconds | 10 |
| `pg_wait_sampling.profile_period` | `int4` | Profile sampling period in milliseconds | 10 |
| `pg_wait_sampling.profile_pid` | `bool` | Collect profile per PID | true |
| `pg_wait_sampling.profile_queries` | `enum` | Collect profile per query | top |
| `pg_wait_sampling.sample_cpu` | `bool` | Sample backends running on CPU | true |

If `pg_wait_sampling.profile_pid` is `false`, the profile is aggregated across all processes and `pid` is always zero.

If `pg_wait_sampling.profile_queries` is `none`, `queryid` is zero in all views. With `top`, only top-level statements get query IDs; with `all`, nested statements are included.

If `pg_wait_sampling.sample_cpu` is `true`, sessions that are not waiting on anything are also sampled and the wait-event columns are `NULL`.

Change these parameters in the configuration file or with `ALTER SYSTEM`, then reload the server configuration for changes to take effect.

Refer to the [PostgreSQL documentation](https://www.postgresql.org/docs/current/monitoring-stats.html#WAIT-EVENT-TABLE) for the list of wait events.

### Configuration

Add the extension to `shared_preload_libraries` and adjust sampling settings if needed:

```conf
# postgresql.conf
shared_preload_libraries = 'pg_stat_statements,pg_wait_sampling'
pg_wait_sampling.history_size  = 10000    # samples kept in history
pg_wait_sampling.profile_period = 50      # ms between profile samples
```

Restart the server and create the extensions:

```sql
CREATE EXTENSION pg_stat_statements;
CREATE EXTENSION pg_wait_sampling;
```

> Note (PostgreSQL 14+): if you do not rely on `pg_stat_statements` for query IDs, set `compute_query_id = 'auto'` (or `'on'`) so `queryid` is populated.

### Example: top queries by wait samples

```sql
SELECT s.query,
       SUM(p.count) AS samples
FROM pg_wait_sampling_profile AS p
JOIN pg_stat_statements AS s ON s.queryid = p.queryid
GROUP BY s.query
ORDER BY samples DESC
LIMIT 5;
```

This query highlights the statements that accumulated the largest number of wait-event samples.

### Load order

Load `pg_stat_statements` before `pg_wait_sampling` in `shared_preload_libraries` to keep query identifiers consistent.

## Contributing

`pg_wait_sampling` is actively developed and tested, but issues may still surface. Report problems or ideas on the [GitHub issue tracker](https://github.com/postgrespro/pg_wait_sampling/issues).

Contributions are welcome. If you plan to implement new functionality, open a discussion or submit a pull request.

## Authors

- Alexander Korotkov <a.korotkov@postgrespro.ru>, Postgres Professional, Moscow, Russia
- Ildus Kurbangaliev <i.kurbangaliev@gmail.com>, Postgres Professional, Moscow, Russia
