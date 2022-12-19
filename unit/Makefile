APPS=unit/test_unit_pio unit/test_unit_probackup

test_unit: $(APPS)


TEST_FILES=unit/test_probackup.o unit/test_pio.o

unit/test_unit_pio: LIBS += -lcunit
unit/test_unit_pio: $(PGPOBJS) unit/pgunit.o unit/test_pio.o
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LIBS) $(PG_LIBS_INTERNAL) -o $@

unit/test_unit_probackup: LIBS += -lcunit
unit/test_unit_probackup: $(PGPOBJS) unit/pgunit.o unit/test_probackup.o
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LIBS) $(PG_LIBS_INTERNAL) -o $@

test_unit_run: test_unit
	(cd unit; ./test_unit_pio)
	(cd unit; ./test_unit_probackup)

test_unit_pub: test_unit_run
	(cd unit ; /bin/sh unit_pub.sh)

clean: test_clean

test_clean:
	rm -rf $(APPS) unit/*.o *~ *.gcda *.gcno *.html *.xml pb.info gmon.out report/
