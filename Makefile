#nanosmg to pipelineDB
MODULE_big = nanomsgtopdb

NANOMSGTOPDB_VERSION=1.0

OBJS = nanomsgtopdb.o 

EXTENSION = nanomsgtopdb
DATA = nanomsgtopdb--1.0.sql
REGRESS = receive nanomsg data and input pipelineDB. 

SHLIB_LINK += $(filter -lm, $(LIBS)) -lnanomsg
USE_PGXS=1
ifdef USE_PGXS
PG_CONFIG =pg_config 
PGXS := $(shell $(PG_CONFIG) --pgxs) 
include $(PGXS)
else
subdir = contrib/nanomsgtopdb
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

distrib:
	rm -f *.o
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
	cd .. ; tar --exclude=.svn -chvzf hash_fdw-$(NANOMSGTOPDB_VERSION).tar.gz hash_fdw
