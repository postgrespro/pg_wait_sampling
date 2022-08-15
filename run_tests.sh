#!/bin/bash

# This is a main testing script for:
#	* regression tests
#	* testgres-based tests
#	* cmocka-based tests
# Copyright (c) 2017, Postgres Professional

set -eux

echo PG_VERSION=$PG_VERSION
echo CHECK_CODE=$CHECK_CODE

status=0

# perform code analysis if necessary
if [ "$CHECK_CODE" = "clang" ]; then
    scan-build --status-bugs make USE_PGXS=1 || status=$?
    exit $status

elif [ "$CHECK_CODE" = "cppcheck" ]; then
    cppcheck \
        --template "{file} ({line}): {severity} ({id}): {message}" \
        --enable=warning,portability,performance \
        --suppress=redundantAssignment \
        --suppress=uselessAssignmentPtrArg \
        --suppress=literalWithCharPtrCompare \
        --suppress=incorrectStringBooleanError \
        --std=c89 *.c *.h 2> cppcheck.log

    if [ -s cppcheck.log ]; then
        cat cppcheck.log
        status=1 # error
    fi

    exit $status
fi

# compile and setup tools for isolation tests for pg12 and pg13
if [ "$PG_VERSION" = "12" ] || [ "$PG_VERSION" = "13" ] ; then
	PWD_SAVED=`pwd`
	git clone https://github.com/postgres/postgres.git \
		-b $(pg_config --version | awk '{print $2}' | sed -r 's/\./_/' | sed -e 's/^/REL_/') \
		--depth=1
	cd postgres
	./configure --prefix=/usr/local --without-readline --without-zlib
	cd src/test/isolation
	make install
	mkdir $(dirname $(pg_config --pgxs))/../../src/test/isolation
	cp isolationtester pg_isolation_regress -t $(dirname $(pg_config --pgxs))/../../src/test/isolation
	cd $PWD_SAVED
fi

# don't forget to "make clean"
make USE_PGXS=1 clean

# initialize database
initdb

# build extension
make USE_PGXS=1 install

# check build
status=$?
if [ $status -ne 0 ]; then exit $status; fi

# add pg_wait_sampling to shared_preload_libraries and restart cluster 'test'
echo "shared_preload_libraries = 'pg_stat_statements,pg_wait_sampling'" >> $PGDATA/postgresql.conf
echo "port = 55435" >> $PGDATA/postgresql.conf
pg_ctl start -l /tmp/postgres.log -w

# check startup
status=$?
if [ $status -ne 0 ]; then cat /tmp/postgres.log; fi

# run regression tests
export PG_REGRESS_DIFF_OPTS="-w -U3" # for alpine's diff (BusyBox)
PGPORT=55435 make USE_PGXS=1 installcheck || status=$?

# show diff if it exists
if test -f regression.diffs; then cat regression.diffs; fi

exit $status
