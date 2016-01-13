#!/bin/bash

#============================================================================
# This is a test script for restore command of pg_arman.
#============================================================================

# Load common rules
. sql/common.sh restore

# Parameters exclusive to this test
SCALE=2

# Create some objects expected by pgbench
pgbench_objs()
{
	NUM=$1
	rm -rf ${TBLSPC_PATH}/pgbench
	mkdir -p ${TBLSPC_PATH}/pgbench
	psql --no-psqlrc -p ${TEST_PGPORT} -d postgres > /dev/null 2>&1 <<EOF
CREATE TABLESPACE pgbench LOCATION '${TBLSPC_PATH}/pgbench';
CREATE DATABASE pgbench TABLESPACE = pgbench;
EOF

	pgbench -i -s ${SCALE} -p ${TEST_PGPORT} -d pgbench > ${TEST_BASE}/pgbench-${NUM}.log 2>&1
}

echo '###### RESTORE COMMAND TEST-0001 ######'
echo '###### recovery to latest from full backup ######'
init_backup
pgbench_objs 0001
pgbench -p ${TEST_PGPORT} -d pgbench > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0001-before.out
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pg_ctl stop -m immediate > /dev/null 2>&1
pg_arman restore -B ${BACKUP_PATH} --quiet;echo $?
pg_ctl start -w -t 600 > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0001-after.out
diff ${TEST_BASE}/TEST-0001-before.out ${TEST_BASE}/TEST-0001-after.out
echo ''

echo '###### RESTORE COMMAND TEST-0002 ######'
echo '###### recovery to latest from full + page backups ######'
init_backup
pgbench_objs 0002
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pgbench -p ${TEST_PGPORT} -d pgbench > /dev/null 2>&1
pg_arman backup -B ${BACKUP_PATH} -b page -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0002-before.out
pg_ctl stop -m immediate > /dev/null 2>&1
pg_arman restore -B ${BACKUP_PATH} --quiet;echo $?
pg_ctl start -w -t 600 > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0002-after.out
diff ${TEST_BASE}/TEST-0002-before.out ${TEST_BASE}/TEST-0002-after.out
echo ''

echo '###### RESTORE COMMAND TEST-0003 ######'
echo '###### recovery to latest from compressed full backup ######'
init_backup
pgbench_objs 0003
pgbench -p ${TEST_PGPORT} -d pgbench > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0003-before.out
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pg_ctl stop -m immediate > /dev/null 2>&1
pg_arman restore -B ${BACKUP_PATH} --quiet;echo $?
pg_ctl start -w -t 600 > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0003-after.out
diff ${TEST_BASE}/TEST-0003-before.out ${TEST_BASE}/TEST-0003-after.out
echo ''

echo '###### RESTORE COMMAND TEST-0004 ######'
echo '###### recovery to target timeline ######'
init_backup
pgbench_objs 0004
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0004-before.out
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
TARGET_TLI=`pg_controldata | grep " TimeLineID:" | awk '{print $4}'`
pg_ctl stop -m immediate > /dev/null 2>&1
pg_arman restore -B ${BACKUP_PATH} --quiet;echo $?
pg_ctl start -w -t 600 > /dev/null 2>&1
pgbench -p ${TEST_PGPORT} -d pgbench > /dev/null 2>&1
pg_ctl stop -m immediate > /dev/null 2>&1
pg_arman restore -B ${BACKUP_PATH} --recovery-target-timeline=${TARGET_TLI} --quiet;echo $?
echo "checking recovery.conf..."
TARGET_TLI_IN_RECOVERY_CONF=`grep "recovery_target_timeline = " ${PGDATA_PATH}/recovery.conf | awk '{print $3}' | sed -e "s/'//g"`
if [ ${TARGET_TLI} = ${TARGET_TLI_IN_RECOVERY_CONF} ]; then
	echo 'OK: recovery.conf has the given target timeline.'
else
	echo 'NG: recovery.conf does not have the given target timeline.'
fi
pg_ctl start -w -t 600 > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0004-after.out
diff ${TEST_BASE}/TEST-0004-before.out ${TEST_BASE}/TEST-0004-after.out
echo ''

echo '###### RESTORE COMMAND TEST-0005 ######'
echo '###### recovery to target time ######'
init_backup
pgbench_objs 0005
pgbench -p ${TEST_PGPORT} -d pgbench > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0005-before.out
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
TARGET_TIME=`date +"%Y-%m-%d %H:%M:%S"`
pgbench -p ${TEST_PGPORT} -d pgbench > /dev/null 2>&1
pg_ctl stop -m immediate > /dev/null 2>&1
pg_arman restore -B ${BACKUP_PATH} --recovery-target-time="${TARGET_TIME}" --quiet;echo $?
pg_ctl start -w -t 600 > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0005-after.out
diff ${TEST_BASE}/TEST-0005-before.out ${TEST_BASE}/TEST-0005-after.out
echo ''

echo '###### RESTORE COMMAND TEST-0006 ######'
echo '###### recovery to target XID ######'
init_backup
pgbench_objs 0006
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "CREATE TABLE tbl0006 (a text);" > /dev/null 2>&1
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pgbench -p ${TEST_PGPORT} pgbench > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0006-before.out
TARGET_XID=`psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -tAq -c "INSERT INTO tbl0006 VALUES ('inserted') RETURNING (xmin);"`
pgbench -p ${TEST_PGPORT} -d pgbench > /dev/null 2>&1
pg_ctl stop -m immediate > /dev/null 2>&1
pg_arman restore -B ${BACKUP_PATH} --recovery-target-xid="${TARGET_XID}" --quiet;echo $?
pg_ctl start -w -t 600 > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0006-after.out
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM tbl0006;" > ${TEST_BASE}/TEST-0006-tbl.dump
diff ${TEST_BASE}/TEST-0006-before.out ${TEST_BASE}/TEST-0006-after.out
if grep "inserted" ${TEST_BASE}/TEST-0006-tbl.dump > /dev/null ; then
	echo 'OK: recovery-target-xid options works well.'
else
	echo 'NG: recovery-target-xid options does not work well.'
fi
echo ''

echo '###### RESTORE COMMAND TEST-0007 ######'
echo '###### recovery with target inclusive false ######'
init_backup
pgbench_objs 0007
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "CREATE TABLE tbl0007 (a text);" > /dev/null 2>&1
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pgbench -p ${TEST_PGPORT} pgbench > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0007-before.out
TARGET_XID=`psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -tAq -c "INSERT INTO tbl0007 VALUES ('inserted') RETURNING (xmin);"`
pgbench -p ${TEST_PGPORT} -d pgbench > /dev/null 2>&1
pg_ctl stop -m immediate > /dev/null 2>&1
pg_arman restore -B ${BACKUP_PATH} --recovery-target-xid="${TARGET_XID}" --recovery-target-inclusive=false --quiet;echo $?
pg_ctl start -w -t 600 > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0007-after.out
psql --no-psqlrc -p ${TEST_PGPORT} -d pgbench -c "SELECT * FROM tbl0007;" > ${TEST_BASE}/TEST-0007-tbl.dump
diff ${TEST_BASE}/TEST-0007-before.out ${TEST_BASE}/TEST-0007-after.out
if grep "inserted" ${TEST_BASE}/TEST-0007-tbl.dump > /dev/null ; then
	echo 'NG: recovery-target-inclusive=false does not work well.'
else
	echo 'OK: recovery-target-inclusive=false works well.'
fi
echo ''

echo '###### RESTORE COMMAND TEST-0008 ######'
echo '###### recovery from page backup after database creation ######'
init_backup
pgbench_objs 0008
pg_ctl start -w -t 600 > /dev/null 2>&1
pg_arman backup -B ${BACKUP_PATH} -b full -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
createdb db0008 -p ${TEST_PGPORT}
pgbench -i -s $SCALE -d db0008 -p ${TEST_PGPORT} > ${TEST_BASE}/TEST-0008-db0008-init.out 2>&1
pg_arman backup -B ${BACKUP_PATH} -b page -Z -p ${TEST_PGPORT} -d postgres --quiet;echo $?
pg_arman validate -B ${BACKUP_PATH} --quiet
pgbench -p ${TEST_PGPORT} -d db0008 >> ${TEST_BASE}/TEST-0008-db0008-init.out 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d db0008 -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0008-before.out
pg_ctl stop -m immediate > /dev/null 2>&1
pg_arman restore -B ${BACKUP_PATH} --quiet;echo $?
pg_ctl start -w -t 600 > /dev/null 2>&1
psql --no-psqlrc -p ${TEST_PGPORT} -d db0008 -c "SELECT * FROM pgbench_branches;" > ${TEST_BASE}/TEST-0008-after.out
diff ${TEST_BASE}/TEST-0008-before.out ${TEST_BASE}/TEST-0008-after.out
echo ''

# clean up the temporal test data
pg_ctl stop -m immediate > /dev/null 2>&1
rm -fr ${PGDATA_PATH}
rm -fr ${BACKUP_PATH}
rm -fr ${ARCLOG_PATH}
rm -fr ${TBLSPC_PATH}
