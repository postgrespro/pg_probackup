PBK_GIT_REPO = https://github.com/postgrespro/pg_probackup

subdir=src/bin/pg_probackup
top_builddir=../../..
include $(top_builddir)/src/Makefile.global

# utils
OBJS = src/utils/configuration.o src/utils/json.o src/utils/logger.o \
	src/utils/parray.o src/utils/pgut.o src/utils/thread.o src/utils/remote.o src/utils/file.o

OBJS += src/archive.o src/backup.o src/catalog.o src/checkdb.o src/configure.o src/data.o \
	src/delete.o src/dir.o src/fetch.o src/help.o src/init.o src/merge.o \
	src/parsexlog.o src/ptrack.o src/pg_probackup.o src/restore.o src/show.o src/stream.o \
	src/util.o src/validate.o src/datapagemap.o src/catchup.o

# borrowed files
OBJS += src/pg_crc.o src/receivelog.o src/streamutil.o \
	src/xlogreader.o

EXTRA_CLEAN = src/pg_crc.c \
	src/receivelog.c src/receivelog.h src/streamutil.c src/streamutil.h \
	src/xlogreader.c src/instr_time.h

# OBJS variable must be finally defined before invoking the include directive
ifneq (,$(wildcard $(top_srcdir)/src/bin/pg_basebackup/walmethods.c))
OBJS += src/walmethods.o
EXTRA_CLEAN += src/walmethods.c src/walmethods.h
endif

PG_CPPFLAGS = -I$(libpq_srcdir) -I$(top_srcdir)/src/port -Isrc -I$(srcdir)/src
ifneq ($(PORTNAME), win32)
PG_CPPFLAGS += $(PTHREAD_CFLAGS)
endif
override CPPFLAGS := -DFRONTEND $(CPPFLAGS) $(PG_CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

src/utils/configuration.o: src/datapagemap.h
src/archive.o: src/instr_time.h
src/backup.o: src/receivelog.h src/streamutil.h

src/instr_time.h: $(top_srcdir)/src/include/portability/instr_time.h
	rm -f $@ && $(LN_S) $(top_srcdir)/src/include/portability/instr_time.h $@
src/pg_crc.c: $(top_srcdir)/src/backend/utils/hash/pg_crc.c
	rm -f $@ && $(LN_S) $(top_srcdir)/src/backend/utils/hash/pg_crc.c $@
src/receivelog.c: $(top_srcdir)/src/bin/pg_basebackup/receivelog.c
	rm -f $@ && $(LN_S) $(top_srcdir)/src/bin/pg_basebackup/receivelog.c $@
ifneq (,$(wildcard $(top_srcdir)/src/bin/pg_basebackup/walmethods.c))
src/receivelog.h: src/walmethods.h $(top_srcdir)/src/bin/pg_basebackup/receivelog.h
else
src/receivelog.h: $(top_srcdir)/src/bin/pg_basebackup/receivelog.h
endif
	rm -f $@ && $(LN_S) $(top_srcdir)/src/bin/pg_basebackup/receivelog.h $@
src/streamutil.c: $(top_srcdir)/src/bin/pg_basebackup/streamutil.c
	rm -f $@ && $(LN_S) $(top_srcdir)/src/bin/pg_basebackup/streamutil.c $@
src/streamutil.h: $(top_srcdir)/src/bin/pg_basebackup/streamutil.h
	rm -f $@ && $(LN_S) $(top_srcdir)/src/bin/pg_basebackup/streamutil.h $@
src/xlogreader.c: $(top_srcdir)/src/backend/access/transam/xlogreader.c
	rm -f $@ && $(LN_S) $(top_srcdir)/src/backend/access/transam/xlogreader.c $@
src/walmethods.c: $(top_srcdir)/src/bin/pg_basebackup/walmethods.c
	rm -f $@ && $(LN_S) $(top_srcdir)/src/bin/pg_basebackup/walmethods.c $@
src/walmethods.h: $(top_srcdir)/src/bin/pg_basebackup/walmethods.h
	rm -f $@ && $(LN_S) $(top_srcdir)/src/bin/pg_basebackup/walmethods.h $@

all: pg_probackup

pg_probackup: $(OBJS) | submake-libpq submake-libpgport submake-libpgfeutils
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_probackup$(X) '$(DESTDIR)$(bindir)/pg_probackup$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_probackup$(X)'
    
ifeq ($(PORTNAME), aix)
	CC=xlc_r
endif

include $(top_srcdir)/src/bin/pg_probackup/packaging/Makefile.pkg
include $(top_srcdir)/src/bin/pg_probackup/packaging/Makefile.repo
include $(top_srcdir)/src/bin/pg_probackup/packaging/Makefile.test
