# contrib/pg_wait_sampling/Makefile

MODULE_big = pg_wait_sampling
OBJS = pg_wait_sampling.o collector.o compat.o

EXTENSION = pg_wait_sampling
EXTVERSION = 1.1
DATA_built = pg_wait_sampling--$(EXTVERSION).sql
DATA = pg_wait_sampling--1.0--1.1.sql

REGRESS = load queries

EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/conf.add
EXTRA_CLEAN = pg_wait_sampling--$(EXTVERSION).sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_wait_sampling
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(EXTENSION)--$(EXTVERSION).sql: setup.sql
	cat $^ > $@

# Prepare the package for PGXN submission
DISTVERSION := $(shell git tag -l | tail -n 1 | cut -d 'v' -f 2)
package: dist dist/$(EXTENSION)-$(DISTVERSION).zip

dist:
	mkdir -p dist

dist/$(EXTENSION)-$(DISTVERSION).zip:
	git archive --format zip --prefix=$(EXTENSION)-$(DISTVERSION)/ --output $@ HEAD
