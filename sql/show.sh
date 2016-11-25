#!/bin/bash

#============================================================================
# This is a test script for show command of pg_probackup.
#============================================================================

# Load common rules
. sql/common.sh show

init_backup

echo '###### SHOW COMMAND TEST-0001 ######'
echo '###### Status DONE and OK ######'
pg_probackup backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_probackup show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0001-show.out.1 2>&1
if grep "DONE" ${TEST_BASE}/TEST-0001-show.out.1 > /dev/null ; then
     echo 'OK: DONE status is shown properly.'
else
     echo 'NG: DONE status is not shown.'
fi

pg_probackup show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0001-show.out.2 2>&1
if grep "OK" ${TEST_BASE}/TEST-0001-show.out.2 > /dev/null ; then
     echo 'OK: OK status is shown properly.'
else
     echo 'NG: OK status is not shown.'
fi
echo ''

echo '###### SHOW COMMAND TEST-0002 ######'
echo '###### Status RUNNING  ######'
init_catalog
pg_probackup backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet &
sleep 1
pg_probackup show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0002-show.out 2>&1
if grep "RUNNING" ${TEST_BASE}/TEST-0002-show.out > /dev/null ; then
     echo 'OK: RUNNING status is shown properly.'
else
     echo 'NG: RUNNING status is not shown.'
fi
counter=0
# Wait for backup to finish properly before moving on to next test
while [[ `pg_probackup show -B ${BACKUP_PATH}` == *"RUNNING"* ]]; do
     if [ $counter -gt 60 ] ; then
          echo "Backup took too long to finish"
          exit 1
     fi
     sleep 1
     counter=$(($counter + 1))
done
echo ''

echo '###### SHOW COMMAND TEST-0003 ######'
echo '###### Status CORRUPT ######'
init_catalog
pg_probackup backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet;echo $?
echo 'remove a file from backup intentionally'
rm -f `find ${BACKUP_PATH} -name postgresql.conf`

pg_probackup show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0003-show.out 2>&1
if grep "CORRUPT" ${TEST_BASE}/TEST-0003-show.out > /dev/null ; then
     echo 'OK: CORRUPT status is shown properly.'
else
     echo 'NG: CORRUPT status is not shown.'
fi
echo ''

# clean up the temporal test data
pg_ctl stop -D ${PGDATA_PATH} -m immediate > /dev/null 2>&1
rm -fr ${PGDATA_PATH}
rm -fr ${BACKUP_PATH}
rm -fr ${ARCLOG_PATH}
rm -fr ${TBLSPC_PATH}
