#!/bin/sh

#============================================================================
# This is a test script for option test of pg_arman.
#============================================================================

BASE_PATH=`pwd`

# Clear environment variables used by pg_arman except $PGDATA.
# List of environment variables is defined in catalog.c.
unset BACKUP_PATH
unset ARCLOG_PATH
unset BACKUP_MODE
unset COMPRESS_DATA
unset KEEP_DATA_GENERATIONS
unset KEEP_DATA_DAYS

export PGDATA=$BASE_PATH/results/sample_database

# Note: not exported
BACKUP_PATH=$BASE_PATH/results/sample_backup2

# Setup backup catalog for backup test.
rm -rf $BACKUP_PATH
cp -rp data/sample_backup $BACKUP_PATH

# general option
pg_arman --help
pg_arman --version

# show option
# required argument check
pg_arman show
pg_arman show -B $BACKUP_PATH

# backup option
# required arguments check
pg_arman backup --verbose
pg_arman backup --verbose -B $BACKUP_PATH

# bad arguments check
pg_arman backup --verbose -B $BACKUP_PATH -b bad

# delete or validate requires DATE
pg_arman delete -B $BACKUP_PATH
pg_arman validate -B $BACKUP_PATH

# invalid configuration file check
echo " = INFINITE" > $BACKUP_PATH/pg_arman.ini
pg_arman backup --verbose -B $BACKUP_PATH
echo "BACKUP_MODE= " > $BACKUP_PATH/pg_arman.ini
pg_arman backup --verbose -B $BACKUP_PATH
echo "BACKUP_MODE=B" > $BACKUP_PATH/pg_arman.ini
pg_arman backup --verbose -B $BACKUP_PATH
echo "COMPRESS_DATA=FOO" > $BACKUP_PATH/pg_arman.ini
pg_arman backup --verbose -B $BACKUP_PATH
echo "TIMELINEID=-1" > $BACKUP_PATH/pg_arman.ini
pg_arman backup --verbose -B $BACKUP_PATH
echo "BACKUP_TARGETS=F" > $BACKUP_PATH/pg_arman.ini
pg_arman backup --verbose -B $BACKUP_PATH

# configuration priorityfile check
echo "BACKUP_MODE=ENV_PATH" > $BACKUP_PATH/pg_arman.ini
mkdir $BACKUP_PATH/conf_path
echo "BACKUP_PATH=$BACKUP_PATH/conf_path" > $BACKUP_PATH/pg_arman.conf
echo "BACKUP_MODE=CONF_PATH" > $BACKUP_PATH/conf_path/pg_arman.ini
mkdir $BACKUP_PATH/comm_path
echo "BACKUP_MODE=COMM_PATH" > $BACKUP_PATH/comm_path/pg_arman.ini
export BACKUP_PATH=$BACKUP_PATH
pg_arman backup --verbose
