#!/bin/bash
set -ev

PATH=/usr/lib/postgresql/$PG_MAJOR/bin:$PATH
export PGDATA=/var/lib/postgresql/$PG_MAJOR/test
export COPT=-Werror
export USE_PGXS=1

sudo mkdir -p /var/lib/postgresql/$PG_MAJOR
sudo chmod 1777 /var/lib/postgresql/$PG_MAJOR
sudo chmod 1777 /var/run/postgresql

make clean
make

sudo -E env PATH=$PATH make install

initdb
echo "shared_preload_libraries = pg_wait_sampling" >> $PGDATA/postgresql.conf

pg_ctl -l logfile start
make installcheck
pg_ctl stop
