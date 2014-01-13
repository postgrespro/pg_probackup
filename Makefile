PROGRAM = pg_rman
SRCS = \
	backup.c \
	catalog.c \
	data.c \
	delete.c \
	dir.c \
	fetch.c \
	init.c \
	parray.c \
	pg_rman.c \
	restore.c \
	show.c \
	status.c \
	util.c \
	validate.c \
	xlog.c \
	pgut/pgut.c \
	pgut/pgut-port.c
OBJS = $(SRCS:.c=.o)

DOCS = doc/pg_rman.txt

# asciidoc and xmlto are present, so install the html documentation and man
# pages as well. html is part of the vanilla documentation. Man pages need a
# special handling at installation.
ifneq ($(ASCIIDOC),)
ifneq ($(XMLTO),)
man_DOCS = doc/pg_rman.1
DOCS += doc/pg_rman.html doc/README.html
endif # XMLTO
endif # ASCIIDOC

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

REGRESS = option init show_validate backup_restore

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_rman
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# remove dependency to libxml2 and libxslt
LIBS := $(filter-out -lxml2, $(LIBS))
LIBS := $(filter-out -lxslt, $(LIBS))

$(OBJS): pg_rman.h

# Part related to documentation
# Compile documentation as well is ASCIIDOC and XMLTO are defined
ifneq ($(ASCIIDOC),)
ifneq ($(XMLTO),)
all: docs
docs:
	$(MAKE) -C doc/

# Special handling for man pages, they need to be in a dedicated folder
install: install-man

install-man:
	$(MKDIR_P) '$(DESTDIR)$(mandir)/man1/'
	$(INSTALL_DATA) $(man_DOCS) '$(DESTDIR)$(mandir)/man1/'
endif # XMLTO
endif # ASCIIDOC

# Clean up documentation as well
clean: clean-docs
clean-docs:
	$(MAKE) -C doc/ clean
