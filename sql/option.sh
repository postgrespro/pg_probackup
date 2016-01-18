#!/bin/bash

#============================================================================
# This is a test script for options of pg_arman.
#============================================================================

# Load common rules
. sql/common.sh option

cleanup

echo '###### COMMAND OPTION TEST-0001 ######'
echo '###### help option ######'
pg_arman --help;echo $?
echo ''

echo '###### COMMAND OPTION TEST-0002 ######'
echo '###### version option ######'
pg_arman --version;echo $?
echo ''

echo '###### COMMAND OPTION TEST-0003 ######'
echo '###### backup command failure without backup path option ######'
pg_arman backup -A ${ARCLOG_PATH} -b full -p ${TEST_PGPORT};echo $?
echo ''

echo '###### COMMAND OPTION TEST-0004 ######'
echo '###### backup command failure without backup mode option ######'
pg_arman backup -B ${BACKUP_PATH} -A ${ARCLOG_PATH} -p ${TEST_PGPORT};echo $?
echo ''

echo '###### COMMAND OPTION TEST-0005 ######'
echo '###### backup command failure with invalid backup mode option ######'
pg_arman backup -B ${BACKUP_PATH} -A ${ARCLOG_PATH} -b bad -p ${TEST_PGPORT};echo $?
echo ''

echo '###### COMMAND OPTION TEST-0006 ######'
echo '###### delete failure without archive path ######'
pg_arman delete -B ${BACKUP_PATH};echo $?
echo ''

echo '###### COMMAND OPTION TEST-0007 ######'
echo '###### delete failure without DATE ######'
pg_arman delete -B ${BACKUP_PATH} -A ${ARCLOG_PATH};echo $?
echo ''

init_backup

echo '###### COMMAND OPTION TEST-0008 ######'
echo '###### syntax error in pg_arman.ini ######'
echo " = INFINITE" >> ${BACKUP_PATH}/pg_arman.ini
pg_arman backup -B ${BACKUP_PATH} -A ${ARCLOG_PATH} -p ${TEST_PGPORT};echo $?
echo ''

echo '###### COMMAND OPTION TEST-0009 ######'
echo '###### invalid value in pg_arman.ini ######'
init_catalog
echo "BACKUP_MODE=" >> ${BACKUP_PATH}/pg_arman.ini
pg_arman backup -B ${BACKUP_PATH} -A ${ARCLOG_PATH} -p ${TEST_PGPORT};echo $?
echo ''

echo '###### COMMAND OPTION TEST-0010 ######'
echo '###### invalid value in pg_arman.ini ######'
init_catalog
echo "KEEP_DATA_GENERATIONS=TRUE" >> ${BACKUP_PATH}/pg_arman.ini
pg_arman backup -B ${BACKUP_PATH} -A ${ARCLOG_PATH} -b full -p ${TEST_PGPORT};echo $?
echo ''

echo '###### COMMAND OPTION TEST-0011 ######'
echo '###### invalid value in pg_arman.ini ######'
init_catalog
echo "SMOOTH_CHECKPOINT=FOO" >> ${BACKUP_PATH}/pg_arman.ini
pg_arman backup -B ${BACKUP_PATH} -A ${ARCLOG_PATH} -b full -p ${TEST_PGPORT};echo $?
echo ''

echo '###### COMMAND OPTION TEST-0012 ######'
echo '###### invalid option in pg_arman.ini ######'
init_catalog
echo "TIMELINEID=1" >> ${BACKUP_PATH}/pg_arman.ini
pg_arman backup -B ${BACKUP_PATH} -A ${ARCLOG_PATH} -b full -p ${TEST_PGPORT};echo $?
echo ''

echo '###### COMMAND OPTION TEST-0013 ######'
echo '###### check priority of several pg_arman.ini files ######'
init_catalog
mkdir -p ${BACKUP_PATH}/conf_path_a
echo "BACKUP_MODE=ENV_PATH" > ${BACKUP_PATH}/pg_arman.ini
echo "BACKUP_MODE=ENV_PATH_A" > ${BACKUP_PATH}/conf_path_a/pg_arman.ini
pg_arman backup -B ${BACKUP_PATH} -A ${ARCLOG_PATH} -p ${TEST_PGPORT};echo $?
echo ''

# clean up the temporal test data
pg_ctl stop -m immediate > /dev/null 2>&1
rm -fr ${PGDATA_PATH}
rm -fr ${BACKUP_PATH}
rm -fr ${ARCLOG_PATH}
rm -fr ${TBLSPC_PATH}
