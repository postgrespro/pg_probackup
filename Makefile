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
	src/xlogreader.o src/walmethods.o

EXTRA_CLEAN = src/pg_crc.c \
	src/receivelog.c src/streamutil.c src/xlogreader.c src/walmethods.c

PG_CPPFLAGS = -I$(libpq_srcdir) -I$(top_srcdir)/src/port -I$(top_srcdir)/src/include/portability -I$(top_srcdir)/src/bin/pg_basebackup -Isrc -I$(srcdir)/src
ifneq ($(PORTNAME), win32)
PG_CPPFLAGS += $(PTHREAD_CFLAGS)
endif
override CPPFLAGS := -DFRONTEND $(CPPFLAGS) $(PG_CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

src/pg_crc.c: $(top_srcdir)/src/backend/utils/hash/pg_crc.c
	rm -f $@ && $(LN_S) $(top_srcdir)/src/backend/utils/hash/pg_crc.c $@
src/receivelog.c: $(top_srcdir)/src/bin/pg_basebackup/receivelog.c
	rm -f $@ && $(LN_S) $(top_srcdir)/src/bin/pg_basebackup/receivelog.c $@
src/streamutil.c: $(top_srcdir)/src/bin/pg_basebackup/streamutil.c
	rm -f $@ && $(LN_S) $(top_srcdir)/src/bin/pg_basebackup/streamutil.c $@
src/xlogreader.c: $(top_srcdir)/src/backend/access/transam/xlogreader.c
	rm -f $@ && $(LN_S) $(top_srcdir)/src/backend/access/transam/xlogreader.c $@
src/walmethods.c: $(top_srcdir)/src/bin/pg_basebackup/walmethods.c
	rm -f $@ && $(LN_S) $(top_srcdir)/src/bin/pg_basebackup/walmethods.c $@

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
