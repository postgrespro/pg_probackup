PROGRAM = pg_probackup
OBJS = src/backup.o src/catalog.o src/configure.o src/data.o \
	src/delete.o src/dir.o src/fetch.o src/help.o src/init.o \
	src/pg_probackup.o src/restore.o src/show.o src/status.o \
	src/util.o src/validate.o src/datapagemap.o src/parsexlog.o \
	src/xlogreader.o src/streamutil.o src/receivelog.o \
	src/archive.o src/utils/parray.o src/utils/pgut.o src/utils/logger.o

EXTRA_CLEAN = datapagemap.c datapagemap.h xlogreader.c receivelog.c receivelog.h streamutil.c streamutil.h logging.h

all: checksrcdir datapagemap.h logging.h receivelog.h streamutil.h pg_probackup

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
ifndef top_srcdir
	@echo "You must have PostgreSQL source tree available to compile."
	@echo "Pass the path to the PostgreSQL source tree to make, in the top_srcdir"
	@echo "variable: \"make top_srcdir=<path to PostgreSQL source tree>\""
	@exit 1
endif
else
subdir=contrib/pg_probackup
top_builddir=../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

PG_CPPFLAGS = -I$(libpq_srcdir) ${PTHREAD_CFLAGS}
override CPPFLAGS := -DFRONTEND $(CPPFLAGS) $(PG_CPPFLAGS)
PG_LIBS = $(libpq_pgport) ${PTHREAD_CFLAGS}

ifeq ($(PORTNAME), aix)
	CC=xlc_r
endif

envtest:
	: top_srcdir=$(top_srcdir)
	: libpq_srcdir = $(libpq_srcdir)

# This rule's only purpose is to give the user instructions on how to pass
# the path to PostgreSQL source tree to the makefile.
.PHONY: checksrcdir
checksrcdir:
ifndef top_srcdir
	@echo "You must have PostgreSQL source tree available to compile."
	@echo "Pass the path to the PostgreSQL source tree to make, in the top_srcdir"
	@echo "variable: \"make top_srcdir=<path to PostgreSQL source tree>\""
	@exit 1
endif

# Those files are symlinked from the PostgreSQL sources.
xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .
datapagemap.c: % : $(top_srcdir)/src/bin/pg_rewind/%
	rm -f $@ && $(LN_S) $< .
datapagemap.h: % : $(top_srcdir)/src/bin/pg_rewind/%
	rm -f  && $(LN_S) $< .
logging.h: % : $(top_srcdir)/src/bin/pg_rewind/%
	rm -f  && $(LN_S) $< .
receivelog.c: % : $(top_srcdir)/src/bin/pg_basebackup/%
	rm -f  && $(LN_S) $< .
receivelog.h: % : $(top_srcdir)/src/bin/pg_basebackup/%
	rm -f  && $(LN_S) $< .
streamutil.c: % : $(top_srcdir)/src/bin/pg_basebackup/%
	rm -f  && $(LN_S) $< .
streamutil.h: % : $(top_srcdir)/src/bin/pg_basebackup/%
	rm -f  && $(LN_S) $< .
