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
	datapagemap.o \
	parsexlog.o \
	xlogreader.o \
	pgut/pgut.o \
	pgut/pgut-port.o

DOCS = doc/pg_arman.txt

EXTRA_CLEAN = datapagemap.c datapagemap.h xlogreader.c

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

all: checksrcdir docs datapagemap.h pg_arman

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

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Part related to documentation
# Compile documentation as well is ASCIIDOC and XMLTO are defined
ifneq ($(ASCIIDOC),)
ifneq ($(XMLTO),)
docs:
	$(MAKE) -C doc/

# Special handling for man pages, they need to be in a dedicated folder
install: install-man

install-man:
	$(MKDIR_P) '$(DESTDIR)$(mandir)/man1/'
	$(INSTALL_DATA) $(man_DOCS) '$(DESTDIR)$(mandir)/man1/'
else
docs:
	@echo "No docs to build"
endif # XMLTO
else
docs:
	@echo "No docs to build"
endif # ASCIIDOC

# Clean up documentation as well
clean: clean-docs
clean-docs:
	$(MAKE) -C doc/ clean
