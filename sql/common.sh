#!/bin/bash

#============================================================================
# Common setup rules for all tests
#============================================================================

TEST_NAME=$1

# Unset environment variables usable by both Postgres and pg_arman
unset PGUSER
unset PGPORT
unset PGDATABASE
unset BACKUP_MODE
unset ARCLOG_PATH
unset BACKUP_PATH
unset SMOOTH_CHECKPOINT
unset KEEP_DATA_GENERATIONS
unset KEEP_DATA_DAYS
unset RECOVERY_TARGET_TIME
unset RECOVERY_TARGET_XID
unset RECOVERY_TARGET_INCLUSIVE
unset RECOVERY_TARGET_TIMELINE

# Data locations
BASE_PATH=`pwd`
TEST_BASE=${BASE_PATH}/results/${TEST_NAME}
PGDATA_PATH=${TEST_BASE}/data
BACKUP_PATH=${TEST_BASE}/backup
ARCLOG_PATH=${TEST_BASE}/arclog
TBLSPC_PATH=${TEST_BASE}/tblspc
TEST_PGPORT=54321
export PGDATA=${PGDATA_PATH}

# Set of utility functions set across scripts to manage the tests
# Check presence of pgbench command and initialize environment
which pgbench > /dev/null 2>&1
ERR_NUM=$?
if [ $ERR_NUM != 0 ]
then
	echo "pgbench is not installed in this environment."
	echo "It is needed in PATH for those regression tests."
	exit 1
fi

function cleanup()
{
	# cleanup environment
	pg_ctl stop -D ${PGDATA_PATH} -m immediate > /dev/null 2>&1
	for folder in ${PGDATA_PATH} ${BACKUP_PATH} ${ARCLOG_PATH} ${TBLSPC_PATH}; do
		rm -rf $folder
		mkdir -p $folder
	done
}

function init_catalog()
{
	rm -fr ${BACKUP_PATH}
	pg_arman init -B ${BACKUP_PATH} --quiet
}

function init_backup()
{
	# cleanup environment
	cleanup

	# create new database cluster
	initdb --no-locale -D ${PGDATA_PATH} > ${TEST_BASE}/initdb.log 2>&1
	cp $PGDATA_PATH/postgresql.conf $PGDATA_PATH/postgresql.conf_org
	    cat << EOF >> $PGDATA_PATH/postgresql.conf
port = ${TEST_PGPORT}
logging_collector = on
wal_level = hot_standby
wal_log_hints = on
archive_mode = on
archive_command = 'cp %p ${ARCLOG_PATH}/%f'
EOF

    # start PostgreSQL
    pg_ctl start -D ${PGDATA_PATH} -w -t 300 > /dev/null 2>&1
    pgbench -i -p ${TEST_PGPORT} -d postgres > ${TEST_BASE}/pgbench.log 2>&1

    # init backup catalog
    init_catalog
}
