#!/bin/bash

#============================================================================
# This is a test script for init command of pg_arman.
#============================================================================

# Load common rules
. sql/common.sh init

# clean and create database cluster
pg_ctl stop -m immediate > /dev/null 2>&1
rm -fr ${PGDATA}
rm -fr ${BACKUP_PATH}
rm -fr ${ARCLOG_PATH} && mkdir -p ${ARCLOG_PATH}

initdb --no-locale > /dev/null 2>&1
cp ${PGDATA}/postgresql.conf ${PGDATA}/postgresql.conf_org
cat << EOF >> ${PGDATA}/postgresql.conf
wal_level = hot_standby
archive_mode = on
archive_command = 'cp "%p" "${ARCLOG_PATH}/%f"'
EOF

echo '###### INIT COMMAND TEST-0001 ######'
echo '###### success with archive_command ######'
pg_arman -B ${BACKUP_PATH} init --quiet;echo $?
find results/init/backup | xargs ls -Fd | sort

echo '###### INIT COMMAND TEST-0002 ######'
echo '###### success with archive_command and log_directory ######'
rm -rf ${BACKUP_PATH}
cp ${PGDATA_PATH}/postgresql.conf_org ${PGDATA_PATH}/postgresql.conf
cat << EOF >> ${PGDATA}/postgresql.conf
wal_level = hot_standby
archive_mode = on
archive_command = 'cp "%p" "${ARCLOG_PATH}/%f"'
log_directory = '${SRVLOG_PATH}'
EOF
pg_arman -B ${BACKUP_PATH} init --quiet;echo $?
find results/init/backup | xargs ls -Fd | sort

echo '###### INIT COMMAND TEST-0003 ######'
echo '###### success without archive_command ######'
rm -rf ${BACKUP_PATH}
cp ${PGDATA_PATH}/postgresql.conf_org ${PGDATA_PATH}/postgresql.conf
cat << EOF >> ${PGDATA}/postgresql.conf
wal_level = hot_standby
archive_mode = on
log_directory = '${SRVLOG_PATH}'
EOF
pg_arman -B ${BACKUP_PATH} init --quiet;echo $?
find results/init/backup | xargs ls -Fd | sort

echo '###### INIT COMMAND TEST-0004 ######'
echo '###### failure with backup catalog already existed ######'
pg_arman -B ${BACKUP_PATH} init;echo $?
echo ''

echo '###### INIT COMMAND TEST-0005 ######'
echo '###### failure with backup catalog should be given as absolute path ######'
rm -rf ${BACKUP_PATH}
pg_arman --backup-path=resuts/init/backup init;echo $?
echo ''


# clean up the temporal test data
pg_ctl stop -m immediate > /dev/null 2>&1
rm -fr ${PGDATA}
rm -fr ${BACKUP_PATH}
rm -fr ${ARCLOG_PATH}
rm -fr ${SRVLOG_PATH}
