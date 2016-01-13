#!/bin/bash

#============================================================================
# This is a test script for backup command of pg_arman.
#============================================================================

# Load common rules
. sql/common.sh backup

init_backup

echo '###### BACKUP COMMAND TEST-0001 ######'
echo '###### full backup mode ######'
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0001.log 2>&1
grep -c OK ${TEST_BASE}/TEST-0001.log
grep OK ${TEST_BASE}/TEST-0001.log | sed -e 's@[^-]@@g' | wc -c | sed 's/^ *//'

echo '###### BACKUP COMMAND TEST-0002 ######'
echo '###### page-level backup mode ######'
pg_arman backup -B ${BACKUP_PATH} -b page -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0002.log 2>&1
grep -c OK ${TEST_BASE}/TEST-0002.log
grep OK ${TEST_BASE}/TEST-0002.log | sed -e 's@[^-]@@g' | wc -c | sed 's/^ *//'

echo '###### BACKUP COMMAND TEST-0003 ######'
echo '###### full backup with compression ######'
init_catalog
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0003.log 2>&1
grep -c OK ${TEST_BASE}/TEST-0003.log
grep OK ${TEST_BASE}/TEST-0003.log | grep -c true
grep OK ${TEST_BASE}/TEST-0003.log | sed -e 's@[^-]@@g' | wc -c | sed 's/^ *//'

echo '###### BACKUP COMMAND TEST-0004 ######'
echo '###### full backup with smooth checkpoint ######'
init_catalog
pg_arman backup -B ${BACKUP_PATH} -b full -C -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0004.log 2>&1
grep -c OK ${TEST_BASE}/TEST-0004.log
grep OK ${TEST_BASE}/TEST-0004.log | sed -e 's@[^-]@@g' | wc -c | sed 's/^ *//'

echo '###### BACKUP COMMAND TEST-0005 ######'
echo '###### full backup with keep-data-generations and keep-data-days ######'
init_catalog
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0005-before.log 2>&1
NUM_OF_FULL_BACKUPS_BEFORE=`grep OK ${TEST_BASE}/TEST-0005-before.log | grep FULL | wc -l | sed 's/^ *//'`
if [ ${NUM_OF_FULL_BACKUPS_BEFORE} -gt 2 ] ; then
	echo "The number of existing full backups validated is greater than 2."
	echo "OK. Let's try to test --keep-data-generations=1."
else
	echo "The number of existing full backups validated is not greater than 2."
	echo "NG. There was something wrong in preparation of this test."
fi
# The actual value of NUM_OF_FULL_BACKUPS_BEFORE can vary on env, so commented out as default.
#echo "Number of existing full backups validated: ${NUM_OF_FULL_BACKUPS_BEFORE}"
grep OK ${TEST_BASE}/TEST-0005-before.log | sed -e 's@[^-]@@g' | wc -c | sed 's/^ *//'
pg_arman backup -B ${BACKUP_PATH} -b full --keep-data-days=-1 --keep-data-generations=1 -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pg_arman show --show-all -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0005-after.log 2>&1
NUM_OF_FULL_BACKUPS_AFTER=`grep OK ${TEST_BASE}/TEST-0005-after.log | grep FULL | wc -l | sed 's/^ *//'`
echo "Number of remaining full backups validated: ${NUM_OF_FULL_BACKUPS_AFTER}"
NUM_OF_DELETED_BACKUPS=`grep DELETED ${TEST_BASE}/TEST-0005-after.log | wc -l | sed 's/^ *//'`
echo "Number of deleted backups : ${NUM_OF_DELETED_BACKUPS}"
grep OK ${TEST_BASE}/TEST-0005-after.log | sed -e 's@[^-]@@g' | wc -c | sed 's/^ *//'

echo '###### BACKUP COMMAND TEST-0006 ######'
echo '###### switch backup mode from page to full ######'
init_catalog
echo 'page-level backup without validated full backup'
pg_arman backup -B ${BACKUP_PATH} -b page -Z -p ${TEST_PGPORT} -d postgres;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0006.log 2>&1
grep OK ${TEST_BASE}/TEST-0006.log | grep FULL | wc -l | sed 's/^ *//'
grep ERROR ${TEST_BASE}/TEST-0006.log | grep INCR | wc -l | sed 's/^ *//'

# cleanup
## clean up the temporal test data
pg_ctl stop -m immediate > /dev/null 2>&1
rm -fr ${PGDATA_PATH}
rm -fr ${BACKUP_PATH}
rm -fr ${ARCLOG_PATH}
rm -fr ${TBLSPC_PATH}
