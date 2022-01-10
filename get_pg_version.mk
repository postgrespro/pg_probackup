# pg_probackup build system
#
# When building pg_probackup, there is a chicken and egg problem:
# 1. We have to define the OBJS list before including the PG makefiles.
# 2. To define this list, we need to know the PG major version.
# 3. But we can find out the postgres version only after including makefiles.
#
# This minimal makefile solves this problem, its only purpose is to
# calculate the version number from which the main build will occur next.
#
# Usage:
# include this line into main makefile
# PG_MAJORVER != $(MAKE) USE_PGXS=$(USE_PGXS) PG_CONFIG=$(PG_CONFIG) --silent --makefile=get_pg_version.mk
#
# Known issues:
# When parent make called with -C and without --no-print-directory, then
# 'make: Leaving directory ...' string will be added (by caller make process) to PG_MAJORVER
# (at least with GNU Make 4.2.1)
#
.PHONY: get_pg_version
get_pg_version:

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_probackup
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

get_pg_version:
	$(info $(MAJORVERSION))

