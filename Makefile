##########################################################################
#
#                foreign-data wrapper for ODBC
#
# Copyright (c) 2011, PostgreSQL Global Development Group
#
# This software is released under the PostgreSQL Licence
#
# Author: Zheng Yang <zhengyang4k@gmail.com>
#
# IDENTIFICATION
#                 odbc_fdw/Makefile
# 
##########################################################################

MODULE_big = odbc_fdw
OBJS = odbc_fdw.o

EXTENSION = odbc_fdw
DATA = odbc_fdw--0.0.1.sql \
  odbc_fdw--0.1.0.sql \
  odbc_fdw--0.0.1--0.1.0.sql \
  odbc_fdw--0.1.0--0.0.1.sql

REGRESS = $(notdir $(basename $(sort $(wildcard test/sql/*test.sql))))
TEST_DIR = test/
REGRESS_OPTS = --inputdir='$(TEST_DIR)' --outputdir='$(TEST_DIR)' --user='postgres'

SHLIB_LINK = -lodbc

ifdef DEBUG
override CFLAGS += -DDEBUG -g -O0
endif

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

run_test:
	# Here should go the parser to change the template values for config values
	make installcheck
