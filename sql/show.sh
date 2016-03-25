#!/bin/bash

#============================================================================
# This is a test script for show command of pg_arman.
#============================================================================

# Load common rules
. sql/common.sh show

init_backup

echo '###### SHOW COMMAND TEST-0001 ######'
echo '###### Status DONE and OK ######'
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0001-show.out.1 2>&1
if grep "DONE" ${TEST_BASE}/TEST-0001-show.out.1 > /dev/null ; then
     echo 'OK: DONE status is shown properly.'
else
     echo 'NG: DONE status is not shown.'
fi
pg_arman validate -B ${BACKUP_PATH} --quiet;echo $?
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0001-show.out.2 2>&1
if grep "OK" ${TEST_BASE}/TEST-0001-show.out.2 > /dev/null ; then
     echo 'OK: OK status is shown properly.'
else
     echo 'NG: OK status is not shown.'
fi
echo ''

echo '###### SHOW COMMAND TEST-0002 ######'
echo '###### Status RUNNING  ######'
init_catalog
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet &
sleep 1
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0002-show.out 2>&1
if grep "RUNNING" ${TEST_BASE}/TEST-0002-show.out > /dev/null ; then
     echo 'OK: RUNNING status is shown properly.'
else
     echo 'NG: RUNNING status is not shown.'
fi
counter=0
while [[ `pg_arman show -B ${BACKUP_PATH}` == *"RUNNING"* ]]
do
     if [ $counter -lt 30 ] ; then
          break
     fi
     sleep 2
     let counter=counter+1
done
echo ''

echo '###### SHOW COMMAND TEST-0003 ######'
echo '###### Status CORRUPT ######'
init_catalog
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet;echo $?
echo 'remove a file from backup intentionally'
rm -f `find ${BACKUP_PATH} -name postgresql.conf`
pg_arman validate -B ${BACKUP_PATH} --quiet > /dev/null 2>&1;echo $?
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0003-show.out 2>&1
if grep "CORRUPT" ${TEST_BASE}/TEST-0003-show.out > /dev/null ; then
     echo 'OK: CORRUPT status is shown properly.'
else
     echo 'NG: CORRUPT status is not shown.'
fi
echo ''

echo '###### SHOW COMMAND TEST-0004 ######'
echo '###### Status DELETED ######'
init_catalog
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet > /dev/null 2>&1;echo $?
DELETE_DATE=`date +"%Y-%m-%d %H:%M:%S"`
pg_arman backup -B ${BACKUP_PATH} -b full -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet > /dev/null 2>&1;echo $?
pg_arman delete ${DELETE_DATE} -B ${BACKUP_PATH} > /dev/null 2>&1;echo $?
pg_arman show -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0004-show.out 2>&1
pg_arman show -a -B ${BACKUP_PATH} > ${TEST_BASE}/TEST-0004-show-all.out 2>&1
if ! grep "DELETED" ${TEST_BASE}/TEST-0004-show.out > /dev/null && grep "DELETED" ${TEST_BASE}/TEST-0004-show-all.out > /dev/null ; then
     echo 'OK: DELETED status is shown properly.'
else
     echo 'NG: DELETED status is not shown.'
fi
echo ''

# clean up the temporal test data
pg_ctl stop -D ${PGDATA_PATH} -m immediate > /dev/null 2>&1
rm -fr ${PGDATA_PATH}
rm -fr ${BACKUP_PATH}
rm -fr ${ARCLOG_PATH}
rm -fr ${TBLSPC_PATH}
