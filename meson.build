# Copyright (c) 2025, Postgres Professional

# Does not support the PGXS infrastructure at this time. Please, compile as part
# of the contrib source tree.

pg_wait_sampling_sources = files(
  'collector.c',
  'pg_wait_sampling.c',
)

if host_system == 'windows'
  pg_wait_sampling_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_wait_sampling',
    '--FILEDESC', 'pg_wait_sampling - provides information about the current wait events for particular processes.',])
endif

pg_wait_sampling = shared_module('pg_wait_sampling',
  pg_wait_sampling_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_wait_sampling

install_data(
  'pg_wait_sampling.control',
  'pg_wait_sampling--1.0--1.1.sql',
  'pg_wait_sampling--1.1.sql',
  'pg_wait_sampling--1.1--1.2.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_wait_sampling',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'load',
      'queries',
    ],
    'regress_args': ['--temp-config', files('conf.add')],
  },
}
