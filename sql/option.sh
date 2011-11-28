#!/bin/sh

#============================================================================
# This is a test script for option test of pg_rman.
#============================================================================

BASE_PATH=`pwd`

# Clear environment variables used by pg_rman except $PGDATA.
# List of environment variables is defined in catalog.c.
unset BACKUP_PATH
unset ARCLOG_PATH
unset SRVLOG_PATH
unset BACKUP_MODE
unset COMPRESS_DATA
unset KEEP_ARCLOG_DAYS
unset KEEP_DATA_GENERATIONS
unset KEEP_DATA_DAYS
unset KEEP_SRVLOG_FILES
unset KEEP_SRVLOG_DAYS

export PGDATA=$BASE_PATH/results/sample_database

# Note: not exported
BACKUP_PATH=$BASE_PATH/results/sample_backup2

# Setup backup catalog for backup test.
rm -rf $BACKUP_PATH
cp -rp data/sample_backup $BACKUP_PATH

# general option
pg_rman --help
pg_rman --version

# backup option
# required arguments check
pg_rman backup --verbose
pg_rman backup --verbose -B $BACKUP_PATH
pg_rman backup --verbose -B $BACKUP_PATH -b f
pg_rman backup --verbose -B $BACKUP_PATH -b i
pg_rman backup --verbose -B $BACKUP_PATH -b a

# bad arguments check
pg_rman backup --verbose -B $BACKUP_PATH -b bad

# delete or validate requires DATE
pg_rman delete -B $BACKUP_PATH
pg_rman validate -B $BACKUP_PATH

# invalid configuration file check
echo " = INFINITE" > $BACKUP_PATH/pg_rman.ini
pg_rman backup --verbose -B $BACKUP_PATH
echo "BACKUP_MODE= " > $BACKUP_PATH/pg_rman.ini
pg_rman backup --verbose -B $BACKUP_PATH
echo "BACKUP_MODE = F#S" > $BACKUP_PATH/pg_rman.ini
pg_rman backup --verbose -B $BACKUP_PATH
echo "BACKUP_MODE = F #comment A" > $BACKUP_PATH/pg_rman.ini
pg_rman backup --verbose -B $BACKUP_PATH
echo "BACKUP_MODE=B" > $BACKUP_PATH/pg_rman.ini
pg_rman backup --verbose -B $BACKUP_PATH
echo "COMPRESS_DATA=FOO" > $BACKUP_PATH/pg_rman.ini
pg_rman backup --verbose -B $BACKUP_PATH
echo "KEEP_ARCLOG_FILES=YES" > $BACKUP_PATH/pg_rman.ini
pg_rman backup --verbose -B $BACKUP_PATH
echo "TIMELINEID=-1" > $BACKUP_PATH/pg_rman.ini
pg_rman backup --verbose -B $BACKUP_PATH
echo "BACKUP_TARGETS=F" > $BACKUP_PATH/pg_rman.ini
pg_rman backup --verbose -B $BACKUP_PATH
echo "BACKUP_MODE='F''\'\\\F'" > $BACKUP_PATH/pg_rman.ini
pg_rman backup --verbose -B $BACKUP_PATH

# configuration priorityfile check
echo "BACKUP_MODE=ENV_PATH" > $BACKUP_PATH/pg_rman.ini
mkdir $BACKUP_PATH/conf_path
echo "BACKUP_PATH=$BACKUP_PATH/conf_path" > $BACKUP_PATH/pg_rman.conf
echo "BACKUP_MODE=CONF_PATH" > $BACKUP_PATH/conf_path/pg_rman.ini
mkdir $BACKUP_PATH/comm_path
echo "BACKUP_MODE=COMM_PATH" > $BACKUP_PATH/comm_path/pg_rman.ini
export BACKUP_PATH=$BACKUP_PATH
pg_rman backup --verbose
