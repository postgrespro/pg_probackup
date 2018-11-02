PROGRAM = pg_probackup

# utils
OBJS = src/utils/json.o src/utils/logger.o src/utils/parray.o \
	src/utils/pgut.o src/utils/thread.o src/utils/remote.o src/utils/file.o

OBJS += src/archive.o src/backup.o src/catalog.o src/configure.o src/data.o \
	src/delete.o src/dir.o src/fetch.o src/help.o src/init.o src/merge.o \
	src/parsexlog.o src/pg_probackup.o src/restore.o src/show.o src/util.o \
	src/validate.o

# borrowed files
OBJS += src/pg_crc.o src/datapagemap.o src/receivelog.o src/streamutil.o \
	src/xlogreader.o

EXTRA_CLEAN = src/pg_crc.c src/datapagemap.c src/datapagemap.h src/logging.h \
	src/receivelog.c src/receivelog.h src/streamutil.c src/streamutil.h \
	src/xlogreader.c

INCLUDES = src/datapagemap.h src/logging.h src/streamutil.h src/receivelog.h

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
# !USE_PGXS
else
subdir=contrib/pg_probackup
top_builddir=../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif # USE_PGXS

ifeq ($(top_srcdir),../..)
 ifeq ($(LN_S),ln -s)
	srchome=$(top_srcdir)/..
 endif
else
srchome=$(top_srcdir)
endif

ifeq (,$(filter 9.5 9.6,$(MAJORVERSION)))
OBJS += src/walmethods.o
EXTRA_CLEAN += src/walmethods.c src/walmethods.h
INCLUDES += src/walmethods.h
endif

PG_CPPFLAGS = -I$(libpq_srcdir) ${PTHREAD_CFLAGS} -Isrc -I$(top_srcdir)/$(subdir)/src
override CPPFLAGS := -DFRONTEND $(CPPFLAGS) $(PG_CPPFLAGS)
PG_LIBS = $(libpq_pgport) ${PTHREAD_CFLAGS}

all: checksrcdir $(INCLUDES);

$(PROGRAM): $(OBJS)

src/datapagemap.c: $(top_srcdir)/src/bin/pg_rewind/datapagemap.c
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_rewind/datapagemap.c $@
src/datapagemap.h: $(top_srcdir)/src/bin/pg_rewind/datapagemap.h
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_rewind/datapagemap.h $@
src/logging.h: $(top_srcdir)/src/bin/pg_rewind/logging.h
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_rewind/logging.h $@
src/pg_crc.c: $(top_srcdir)/src/backend/utils/hash/pg_crc.c
	rm -f $@ && $(LN_S) $(srchome)/src/backend/utils/hash/pg_crc.c $@
src/receivelog.c: $(top_srcdir)/src/bin/pg_basebackup/receivelog.c
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/receivelog.c $@
src/receivelog.h: $(top_srcdir)/src/bin/pg_basebackup/receivelog.h
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/receivelog.h $@
src/streamutil.c: $(top_srcdir)/src/bin/pg_basebackup/streamutil.c
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/streamutil.c $@
src/streamutil.h: $(top_srcdir)/src/bin/pg_basebackup/streamutil.h
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/streamutil.h $@
src/xlogreader.c: $(top_srcdir)/src/backend/access/transam/xlogreader.c
	rm -f $@ && $(LN_S) $(srchome)/src/backend/access/transam/xlogreader.c $@


ifeq (,$(filter 9.5 9.6,$(MAJORVERSION)))
src/walmethods.c: $(top_srcdir)/src/bin/pg_basebackup/walmethods.c
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/walmethods.c $@
src/walmethods.h: $(top_srcdir)/src/bin/pg_basebackup/walmethods.h
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/walmethods.h $@
endif

ifeq ($(PORTNAME), aix)
	CC=xlc_r
endif

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
