PROGRAM = pg_arman
OBJS = backup.o \
	catalog.o \
	data.o \
	delete.o \
	dir.o \
	fetch.o \
	init.o \
	parray.o \
	pg_arman.o \
	restore.o \
	show.o \
	status.o \
	util.o \
	validate.o \
	xlog.o \
	pgut/pgut.o \
	pgut/pgut-port.o

DOCS = doc/pg_arman.txt

# asciidoc and xmlto are present, so install the html documentation and man
# pages as well. html is part of the vanilla documentation. Man pages need a
# special handling at installation.
ifneq ($(ASCIIDOC),)
ifneq ($(XMLTO),)
man_DOCS = doc/pg_arman.1
DOCS += doc/pg_arman.html doc/README.html
endif # XMLTO
endif # ASCIIDOC

PG_CPPFLAGS = -I$(libpq_srcdir)
override CPPFLAGS := -DFRONTEND $(CPPFLAGS)
PG_LIBS = $(libpq_pgport)

REGRESS = init option show delete backup restore

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

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
