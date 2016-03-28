#!/bin/bash

#============================================================================
# This is a test script for delete command of pg_arman.
#============================================================================

# Load common rules
. sql/common.sh delete

init_backup
echo '###### DELETE COMMAND TEST-0001 ######'
echo '###### delete full backups ######'
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet
FIRST_BACKUP_DATE=$(get_time_last_backup)
pgbench -p ${TEST_PGPORT} >> ${TEST_BASE}/pgbench.log 2>&1
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet
SECOND_BACKUP_DATE=$(get_time_last_backup)
pgbench -p ${TEST_PGPORT} >> ${TEST_BASE}/pgbench.log 2>&1
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet
THIRD_BACKUP_DATE=$(get_time_last_backup)
pg_arman validate -B ${BACKUP_PATH} --quiet

echo "try to delete the oldest backup"
pg_arman -B ${BACKUP_PATH} delete ${SECOND_BACKUP_DATE} > /dev/null 2>&1
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0001.out.1 2>&1
pg_arman show -a -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0001.out.2 2>&1
grep -c OK ${TEST_BASE}/TEST-0001.out.1
grep -c DELETED ${TEST_BASE}/TEST-0001.out.2
NUM_OF_DELETED_BACKUPS=`grep DELETED ${TEST_BASE}/TEST-0001.out.2 | wc -l | sed 's/^ *//'`
echo "Number of deleted backups should be 1, is it so?: ${NUM_OF_DELETED_BACKUPS}"

init_backup
echo '###### DELETE COMMAND TEST-0002 ######'
echo '###### keep backups which are necessary for recovery ######'
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet
FIRST_BACKUP_DATE=$(get_time_last_backup)
pgbench -p ${TEST_PGPORT} >> ${TEST_BASE}/pgbench.log 2>&1
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet
SECOND_BACKUP_DATE=$(get_time_last_backup)
pg_arman validate -B ${BACKUP_PATH} --quiet
pgbench -p ${TEST_PGPORT} >> ${TEST_BASE}/pgbench.log 2>&1
pg_arman backup -B ${BACKUP_PATH} -b page -p ${TEST_PGPORT} -d postgres --quiet
THIRD_BACKUP_DATE=$(get_time_last_backup)
pg_arman validate -B ${BACKUP_PATH} --quiet
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet
FOURTH_BACKUP_DATE=$(get_time_last_backup)
pg_arman validate -B ${BACKUP_PATH} --quiet

echo "try to delete before third backup"
pg_arman delete -B ${BACKUP_PATH} ${THIRD_BACKUP_DATE} > /dev/null 2>&1
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0002.out.1 2>&1
pg_arman show -a -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0002.out.2 2>&1
grep -c OK ${TEST_BASE}/TEST-0002.out.1
grep -c DELETED ${TEST_BASE}/TEST-0002.out.2
NUM_OF_DELETED_BACKUPS=`grep DELETED ${TEST_BASE}/TEST-0002.out.2 | wc -l | sed 's/^ *//'`
echo "Number of deleted backups should be 1, is it so?: ${NUM_OF_DELETED_BACKUPS}"

init_backup
# clean up the temporal test data
pg_ctl stop -m immediate > /dev/null 2>&1
rm -fr ${PGDATA_PATH}
rm -fr ${BACKUP_PATH}
rm -fr ${ARCLOG_PATH}
rm -fr ${SRVLOG_PATH}
rm -fr ${TBLSPC_PATH}
