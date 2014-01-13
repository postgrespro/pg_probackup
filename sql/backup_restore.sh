#!/bin/sh

BASE_PATH=`pwd`
export PGDATA=$BASE_PATH/results/sample_database

export BACKUP_PATH=$BASE_PATH/results/sample_backup2
export ARCLOG_PATH=$BASE_PATH/results/arclog
export SRVLOG_PATH=$PGDATA/pg_log
export COMPRESS_DATA=YES
XLOG_PATH=$PGDATA/pg_xlog
TBLSPC_PATH=$BASE_PATH/results/tblspc

# Port used for test database cluster
TEST_PGPORT=54321

# configuration
SCALE=1
DURATION=10
ISOLATE_SRVLOG=0
ISOLATE_WAL=0

while [ $# -gt 0 ]; do
	case $1 in
		"-s")
			ISOLATE_SRVLOG=1
			shift
			;;
		"-w")
			ISOLATE_WAL=1
			shift
			;;
		"-d")
			DURATION=`expr $2 + 0`
			if [ $? -ne 0 ]; then
				echo "invalid duration"
				exit 1
			fi
			shift 2
			;;
		"-s")
			SCALE=`expr $2 + 0`
			if [ $? -ne 0 ]; then
				echo "invalid scale"
				exit 1
			fi
			shift 2
			;;
		*)
			shift
			;;
	esac
done

# delete old database cluster
pg_ctl stop -m immediate > /dev/null 2>&1
rm -rf $PGDATA
rm -rf $BASE_PATH/results/pg_xlog
rm -rf $BASE_PATH/results/srvlog
rm -rf $ARCLOG_PATH
rm -rf $SRVLOG_PATH
rm -rf $TBLSPC_PATH

# create new backup catalog
rm -rf $BACKUP_PATH
pg_rman init -B $BACKUP_PATH --quiet

# create default configuration file
cat << EOF > $BACKUP_PATH/pg_rman.ini
  # comment
  BACKUP_MODE = F # comment
EOF

# create new database cluster
initdb --no-locale > $BASE_PATH/results/initdb.log 2>&1
cat << EOF >> $PGDATA/postgresql.conf
port = $TEST_PGPORT
logging_collector = on
wal_level = archive
archive_mode = on
archive_command = 'cp "%p" "$ARCLOG_PATH/%f"'
log_filename = 'postgresql-%F_%H%M%S.log'
EOF

mkdir -p $ARCLOG_PATH
mkdir -p $TBLSPC_PATH

# determine serverlog directory
if [ "$ISOLATE_SRVLOG" -ne 0 ]; then
	export SRVLOG_PATH=$BASE_PATH/results/srvlog
	echo "log_directory = '$SRVLOG_PATH'" >> $PGDATA/postgresql.conf
	mkdir -p $SRVLOG_PATH
else
	export SRVLOG_PATH=$PGDATA/pg_log
	echo "log_directory = 'pg_log'" >> $PGDATA/postgresql.conf
fi

# isolate online WAL
if [ "$ISOLATE_WAL" -ne 0 ]; then
	XLOG_PATH=$BASE_PATH/results/pg_xlog
	mv $PGDATA/pg_xlog $XLOG_PATH
	ln -s $XLOG_PATH $PGDATA/pg_xlog
fi

# start PostgreSQL
pg_ctl start -w -t 3600 > /dev/null 2>&1

# create tablespace and database for pgbench
mkdir -p $TBLSPC_PATH/pgbench
psql --no-psqlrc -p $TEST_PGPORT postgres <<EOF
CREATE TABLESPACE pgbench LOCATION '$TBLSPC_PATH/pgbench';
CREATE DATABASE pgbench TABLESPACE = pgbench;
EOF

# data_delete
export KEEP_DATA_GENERATIONS=2
export KEEP_DATA_DAYS=0
for i in `seq 1 5`; do
#	pg_rman -p $TEST_PGPORT backup --verbose -d postgres > $BASE_PATH/results/log_full_0_$i 2>&1
	pg_rman -w -p $TEST_PGPORT backup --verbose -d postgres > $BASE_PATH/results/log_full_0_$i 2>&1
done
pg_rman -p $TEST_PGPORT show `date +%Y` -a --verbose -d postgres > $BASE_PATH/results/log_show_d_1 2>&1
echo "# of deleted backups"
grep -c DELETED $BASE_PATH/results/log_show_d_1

pgbench -p $TEST_PGPORT -i -s $SCALE pgbench > $BASE_PATH/results/pgbench.log 2>&1

echo "full database backup"
psql --no-psqlrc -p $TEST_PGPORT postgres -c "checkpoint"
#pg_rman -p $TEST_PGPORT backup --verbose -d postgres > $BASE_PATH/results/log_full_1 2>&1
pg_rman -w -p $TEST_PGPORT backup --verbose -d postgres > $BASE_PATH/results/log_full_1 2>&1

pgbench -p $TEST_PGPORT -T $DURATION -c 10 pgbench >> $BASE_PATH/results/pgbench.log 2>&1
echo "incremental database backup"
psql --no-psqlrc -p $TEST_PGPORT postgres -c "checkpoint"
#pg_rman -p $TEST_PGPORT backup -b i --verbose -d postgres > $BASE_PATH/results/log_incr1 2>&1
pg_rman -w -p $TEST_PGPORT backup -b i --verbose -d postgres > $BASE_PATH/results/log_incr1 2>&1

# validate all backup
pg_rman validate `date +%Y` --verbose > $BASE_PATH/results/log_validate1 2>&1
pg_rman -p $TEST_PGPORT show `date +%Y` -a --verbose -d postgres > $BASE_PATH/results/log_show0 2>&1
pg_dumpall > $BASE_PATH/results/dump_before_rtx.sql
target_xid=`psql --no-psqlrc -p $TEST_PGPORT pgbench -tAq -c "INSERT INTO pgbench_history VALUES (1) RETURNING(xmin);"`
psql --no-psqlrc -p $TEST_PGPORT postgres -c "checkpoint"
#pg_rman -p $TEST_PGPORT backup -b i --verbose -d postgres > $BASE_PATH/results/log_incr2 2>&1
pg_rman -w -p $TEST_PGPORT backup -b i --verbose -d postgres > $BASE_PATH/results/log_incr2 2>&1

pgbench -p $TEST_PGPORT -T $DURATION -c 10 pgbench >> $BASE_PATH/results/pgbench.log 2>&1
echo "archived WAL and serverlog backup"
#pg_rman -p $TEST_PGPORT backup -b a --verbose -d postgres > $BASE_PATH/results/log_arclog 2>&1
pg_rman -w -p $TEST_PGPORT backup -b a --verbose -d postgres > $BASE_PATH/results/log_arclog 2>&1

# stop PG during transaction and get commited info for verifing
echo "stop DB during running pgbench"
pgbench -p $TEST_PGPORT -T $DURATION -c 10 pgbench >> $BASE_PATH/results/pgbench.log 2>&1 &
sleep `expr $DURATION / 2`
pg_ctl stop -m immediate > /dev/null 2>&1
cp -rp $PGDATA $PGDATA.bak
pg_ctl start -w -t 3600 > /dev/null 2>&1
pg_dumpall > $BASE_PATH/results/dump_before.sql

# revert to crushed cluster
pg_ctl stop > /dev/null 2>&1
rm -rf $PGDATA
mv $PGDATA.bak $PGDATA

# validate all backup
pg_rman validate `date +%Y` --verbose > $BASE_PATH/results/log_validate2 2>&1

# restore check with pg_rman
pg_rman restore -! --verbose --check > $BASE_PATH/results/log_restore_check_1 2>&1

# restore with pg_rman
CUR_TLI=`pg_controldata | grep "Latest checkpoint's TimeLineID:" | awk '{print $4}'`
pg_rman restore -! --verbose > $BASE_PATH/results/log_restore1_1 2>&1
CUR_TLI_R=`grep "current timeline ID = " $BASE_PATH/results/log_restore1_1 | awk '{print $5}'`
TARGET_TLI=`grep "target timeline ID = " $BASE_PATH/results/log_restore1_1 | awk '{print $5}'`
if [ "$CUR_TLI" != "$CUR_TLI_R" ]; then
	echo "failed: bad timeline ID" CUR_TLI=$CUR_TLI CUR_TLI_R=$CUR_TLI_R
fi

# Backup of online-WAL and serverlog.
echo "diff files in BACKUP_PATH/backup/pg_xlog"
diff -r $PGDATA/pg_xlog $BACKUP_PATH/backup/pg_xlog
echo "# of files in BACKUP_PATH/backup/srvlog"
find $BACKUP_PATH/backup/srvlog -type f | wc -l

# recovery database
pg_ctl start -w -t 3600 > /dev/null 2>&1

# re-restore with pg_rman
pg_ctl stop -m immediate > /dev/null 2>&1

# restore check with pg_rman
pg_rman restore -! --verbose --check > $BASE_PATH/results/log_restore_check_2 2>&1

CUR_TLI=`pg_controldata | grep "Latest checkpoint's TimeLineID:" | awk '{print $4}'`
pg_rman restore -! --verbose > $BASE_PATH/results/log_restore1_2 2>&1
CUR_TLI_R=`grep "current timeline ID = " $BASE_PATH/results/log_restore1_2 | awk '{print $5}'`
TARGET_TLI=`grep "target timeline ID = " $BASE_PATH/results/log_restore1_2 | awk '{print $5}'`
if [ "$CUR_TLI" != "$CUR_TLI_R" ]; then
	echo "failed: bad timeline ID" CUR_TLI=$CUR_TLI CUR_TLI_R=$CUR_TLI_R
fi

# Backup of online-WAL and serverlog.
echo "diff files in BACKUP_PATH/backup/pg_xlog"
diff -r $PGDATA/pg_xlog $BACKUP_PATH/backup/pg_xlog
echo "# of files in BACKUP_PATH/backup/srvlog"
find $BACKUP_PATH/backup/srvlog -type f | wc -l

# re-recovery database
pg_ctl start -w -t 3600 > /dev/null 2>&1

# compare recovery results
pg_dumpall > $BASE_PATH/results/dump_after.sql
diff $BASE_PATH/results/dump_before.sql $BASE_PATH/results/dump_after.sql

# take a backup and delete backed up online files
# incrementa backup can't find last full backup because new timeline started.
echo "full database backup after recovery"
psql --no-psqlrc -p $TEST_PGPORT postgres -c "checkpoint"
#pg_rman -p $TEST_PGPORT backup -b f --verbose -d postgres > $BASE_PATH/results/log_full2 2>&1
pg_rman -w -p $TEST_PGPORT backup -b f --verbose -d postgres > $BASE_PATH/results/log_full2 2>&1

# Backup of online-WAL should been deleted, but serverlog remain.
echo "# of files in BACKUP_PATH/backup/pg_xlog"
find $BACKUP_PATH/backup/pg_xlog -type f | wc -l
echo "# of files in BACKUP_PATH/backup/srvlog"
find $BACKUP_PATH/backup/srvlog -type f | wc -l

# Symbolic links in $ARCLOG_PATH should be deleted.
echo "# of symbolic links in ARCLOG_PATH"
find $ARCLOG_PATH -type l | wc -l

# timeline history files are backed up.
echo "# of files in BACKUP_PATH/timeline_history"
find $BACKUP_PATH/timeline_history -type f | wc -l

# restore with pg_rman
pg_ctl stop -m immediate > /dev/null 2>&1

# restore check with pg_rman
pg_rman restore -! --verbose --check > $BASE_PATH/results/log_restore_check_3 2>&1

CUR_TLI=`pg_controldata | grep "Latest checkpoint's TimeLineID:" | awk '{print $4}'`
pg_rman restore -! --recovery-target-xid $target_xid --recovery-target-inclusive false --verbose > $BASE_PATH/results/log_restore2 2>&1
CUR_TLI_R=`grep "current timeline ID = " $BASE_PATH/results/log_restore2 | awk '{print $5}'`
TARGET_TLI=`grep "target timeline ID = " $BASE_PATH/results/log_restore2 | awk '{print $5}'`
if [ "$CUR_TLI" != "$CUR_TLI_R" ]; then
	echo "failed: bad timeline ID" CUR_TLI=$CUR_TLI CUR_TLI_R=$CUR_TLI_R
fi
echo "# of recovery target option in recovery.conf"
grep -c "recovery_target_" $PGDATA/recovery.conf

# recovery database
pg_ctl start -w -t 3600 > /dev/null 2>&1

pg_dumpall > $BASE_PATH/results/dump_after_rtx.sql
diff $BASE_PATH/results/dump_before_rtx.sql $BASE_PATH/results/dump_after_rtx.sql

# show
pg_rman -p $TEST_PGPORT show --verbose -a -d postgres > $BASE_PATH/results/log_show_timeline_1 2>&1
pg_rman -p $TEST_PGPORT show `date +%Y` -a --verbose -d postgres > $BASE_PATH/results/log_show_timeline_2 2>&1
pg_rman -p $TEST_PGPORT show `date +%Y` --verbose -d postgres > $BASE_PATH/results/log_show_timeline_3 2>&1
echo "# of deleted backups (show all)"
grep -c DELETED $BASE_PATH/results/log_show_timeline_2
echo "# of deleted backups"
grep -c DELETED $BASE_PATH/results/log_show_timeline_3

echo "delete backup"
pg_rman -p $TEST_PGPORT delete --debug -d postgres > $BASE_PATH/results/log_delete1 2>&1
pg_rman -p $TEST_PGPORT show `date +%Y` -a --verbose -d postgres > $BASE_PATH/results/log_show1 2>&1
echo "# of deleted backups"
grep -c DELETED $BASE_PATH/results/log_show1
pg_rman -p $TEST_PGPORT delete `date "+%Y-%m-%d %T"` --debug -d postgres > $BASE_PATH/results/log_delete2 2>&1
pg_rman -p $TEST_PGPORT show `date +%Y` -a --verbose -d postgres > $BASE_PATH/results/log_show2 2>&1
echo "# of deleted backups"
grep -c DELETED $BASE_PATH/results/log_show2
pg_rman -p $TEST_PGPORT show `date +%Y` -a --verbose -d postgres > $BASE_PATH/results/log_show_timeline_4 2>&1

# cleanup
pg_ctl stop -m immediate > /dev/null 2>&1
