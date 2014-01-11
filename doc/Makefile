
manpages = pg_rman.1

EXTRA_DIST = pg_rman.txt Makefile $(manpages)

htmls = pg_rman.html README.html

# We have asciidoc and xmlto, so build everything and define correct
# rules for build.
ifneq ($(ASCIIDOC),)
ifneq ($(XMLTO),)
dist_man_MANS = $(manpages)
doc_DATA = $(htmls)

pg_rman.1: pg_rman.xml $(doc_DATA)
	$(XMLTO) man $<

%.xml: %.txt
	$(ASCIIDOC) -b docbook -d manpage -o $@ $<

%.html: %.txt
	$(ASCIIDOC) -a toc -o $@ $<

README.html: ../README
	$(ASCIIDOC) -a toc -o $@ $<

endif # XMLTO
endif # ASCIIDOC

clean:
	rm -rf $(manpages) *.html *.xml

.PHONY: clean
