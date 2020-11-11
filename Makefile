PROGRAM = pg_probackup

# utils
OBJS = src/utils/configuration.o src/utils/json.o src/utils/logger.o \
	src/utils/parray.o src/utils/pgut.o src/utils/thread.o src/utils/remote.o src/utils/file.o

OBJS += src/archive.o src/backup.o src/catalog.o src/checkdb.o src/configure.o src/data.o \
	src/delete.o src/dir.o src/fetch.o src/help.o src/init.o src/merge.o \
	src/parsexlog.o src/ptrack.o src/pg_probackup.o src/restore.o src/show.o src/stream.o \
	src/util.o src/validate.o src/datapagemap.o

# borrowed files
OBJS += src/pg_crc.o src/receivelog.o src/streamutil.o \
	src/xlogreader.o

EXTRA_CLEAN = src/pg_crc.c \
	src/receivelog.c src/receivelog.h src/streamutil.c src/streamutil.h \
	src/xlogreader.c src/instr_time.h

ifdef top_srcdir
srchome := $(abspath $(top_srcdir))
else
top_srcdir=../..
ifneq (,$(wildcard ../../../contrib/pg_probackup))
# separate build directory support
srchome := $(abspath $(top_srcdir)/..)
else
srchome := $(abspath $(top_srcdir))
endif
endif

# OBJS variable must be finally defined before invoking the include directive
ifneq (,$(wildcard $(srchome)/src/bin/pg_basebackup/walmethods.c))
OBJS += src/walmethods.o
EXTRA_CLEAN += src/walmethods.c src/walmethods.h
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir=contrib/pg_probackup
top_builddir=../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

PG_CPPFLAGS = -I$(libpq_srcdir) ${PTHREAD_CFLAGS} -Isrc -I$(srchome)/$(subdir)/src
override CPPFLAGS := -DFRONTEND $(CPPFLAGS) $(PG_CPPFLAGS)
PG_LIBS_INTERNAL = $(libpq_pgport) ${PTHREAD_CFLAGS}

src/utils/configuration.o: src/datapagemap.h
src/archive.o: src/instr_time.h
src/backup.o: src/receivelog.h src/streamutil.h

src/instr_time.h: $(srchome)/src/include/portability/instr_time.h
	rm -f $@ && $(LN_S) $(srchome)/src/include/portability/instr_time.h $@
src/pg_crc.c: $(srchome)/src/backend/utils/hash/pg_crc.c
	rm -f $@ && $(LN_S) $(srchome)/src/backend/utils/hash/pg_crc.c $@
src/receivelog.c: $(srchome)/src/bin/pg_basebackup/receivelog.c
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/receivelog.c $@
ifneq (,$(wildcard $(srchome)/src/bin/pg_basebackup/walmethods.c))
src/receivelog.h: src/walmethods.h $(srchome)/src/bin/pg_basebackup/receivelog.h
else
src/receivelog.h: $(srchome)/src/bin/pg_basebackup/receivelog.h
endif
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/receivelog.h $@
src/streamutil.c: $(srchome)/src/bin/pg_basebackup/streamutil.c
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/streamutil.c $@
src/streamutil.h: $(srchome)/src/bin/pg_basebackup/streamutil.h
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/streamutil.h $@
src/xlogreader.c: $(srchome)/src/backend/access/transam/xlogreader.c
	rm -f $@ && $(LN_S) $(srchome)/src/backend/access/transam/xlogreader.c $@
src/walmethods.c: $(srchome)/src/bin/pg_basebackup/walmethods.c
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/walmethods.c $@
src/walmethods.h: $(srchome)/src/bin/pg_basebackup/walmethods.h
	rm -f $@ && $(LN_S) $(srchome)/src/bin/pg_basebackup/walmethods.h $@

ifeq ($(PORTNAME), aix)
	CC=xlc_r
endif
