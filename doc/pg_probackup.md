## Name

pg_probackup — backup and recovery manager for PostgreSQL.

## Synopsis
```
pg_probackup [option...] init
pg_probackup [option...] backup
pg_probackup [option...] restore [backup_ID]
pg_probackup [option...] validate [backup_ID]
pg_probackup [option...] show    [backup_ID]
pg_probackup [option...] delete   backup_ID
pg_probackup [option...] delwal  [backup_ID]
pg_probackup [option...] retention show|purge
```

## Description

pg_probackup is an utility to manage backup and recovery of PostgreSQL clusters.
Versions from 9.5 and newer are supported.

The utility makes a binary copy of database cluster files, almost like pg\_basebackup does.
However pg\_probackup provides additional features, required for implementing different backup
strategies and dealing with large amount of data:
* Single backup catalog for managing backups, including multi-server replication configurations.
* Support for parallel backup and restore.
* Support for page-level incremental backups.
* Consistency checks for database cluster files and backups.

pg\_probackup understands the structure of database cluster files and works on page level to store
only meaningful parts of data pages in backups, and also to check data consistency when checksums
are enabled. Backups are also checked for correctness to detect possible disk failures.

Backups along with additional meta-information are created in a special backup directory.
Continuous archiving should be directed to that directory too. Backup directory must be accessible
in the file system of database server; owner of PostgreSQL process must have full access to contents
of this directory. Usual practice is to place backup directory on a separate server, in which case
some network file system should be used.

The same backup directory can be used simultaneously by several PostgreSQL servers with replication
configured between them. Backups can be made from either primary or standby server, and managed in a
single backup strategy.

## Usage

### Initial setup
In any usage scenario, first of all PostgreSQL server should be configured and backup catalog
should be initialized.

pg\_probackup initial setup, as well as further work with the utility, is performed by PostgreSQL
process owner (usually postgres).

A connection to database server is required for pg\_probackup to take backups. Database user,
which pg\_probackup is connected as, must have sufficient privileges to execute some administrative
functions. The user must also have REPLICATION attribute in order to make autonomous backups.
pg\_probackup can be connected as a superuser, but it is advisable to create a separate user with
the following minimum required privileges:
```
CREATE ROLE backup WITH LOGIN REPLICATION;
GRANT USAGE ON SCHEMA pg_catalog TO backup;
GRANT EXECUTE ON FUNCTION current_setting(text) TO backup;
GRANT EXECUTE ON FUNCTION pg_is_in_recovery() TO backup;
GRANT EXECUTE ON FUNCTION pg_start_backup(text, boolean, boolean) TO backup;
GRANT EXECUTE ON FUNCTION pg_stop_backup() TO backup;
GRANT EXECUTE ON FUNCTION pg_stop_backup(boolean) TO backup;
GRANT EXECUTE ON FUNCTION pg_switch_xlog() TO backup;
GRANT EXECUTE ON FUNCTION txid_current() TO backup;
GRANT EXECUTE ON FUNCTION txid_current_snapshot() TO backup;
GRANT EXECUTE ON FUNCTION txid_snapshot_xmax(txid_snapshot) TO backup;
```

When using Postgres Pro server, additional privileges are required for taking incremental backups:
```
GRANT EXECUTE ON FUNCTION pg_ptrack_clear() TO backup;
GRANT EXECUTE ON FUNCTION pg_ptrack_get_and_clear(oid, oid) TO backup;
```

PostgreSQL server configuration must accept connections for the user in pg\_hba.conf.
For autonomous backups, replication connections must also be accepted, and [max\_wal\_senders](https://postgrespro.com/docs/postgresql/current/runtime-config-replication.html#guc-max-wal-senders)
value should be high enough to allow pg\_probackup to connect for streaming WAL files during backup.

[Wal_level](https://postgrespro.com/docs/postgresql/current/runtime-config-wal.html#guc-wal-level) parameter
must be replica of higher (archive for versions below 9.5).

To initialize backup directory, execute the following command:
```
pg_probackup init -B backup_directory -D data_dir
```

The -B option specifies the directory where backups and meta-information will be stored.
As this option is required for all pg\_probackup commands, it makes sense to specify once
it in the BACKUP\_PATH environmental variable.

The -D option specifies the database cluster's data directory. It is handy to put it in PGDATA environmental
variable to not specify it every time in command line. In the subsequent examples these options are omitted.

The utility creates the specified directory and all the necessary files and subdirectories in it:

* pg\_probackup.conf — configuration file with default values for some of the options. Full list of options see below.
* wal/ — directory for WAL files;
* backups/ — directory for backups. The utility will create separate subdirectories for each backup it take,
named by the backup identifier.

The backup directory can be created beforehand, but in this case it must be empty.

### Autonomous Backups
Autonomous backups offer the simplest way to make a backup without need to configure PostgreSQL for
continuous archiving. Such backups contain database cluster files as well as WAL files necessary for recovery.

Without WAL files archive, database cluster can be restored using an autonomous backup only to its state
at the moment the backup was taken.

To make an autonomous backup, execute the following command:
```
pg_probackup backup -b full --stream
```

Additionally this command should be supplied with connection options. These options are specified exactly
the same way as for other PostgreSQL utilities: either by command
line options (-h/--host, -p/--port, -U/--username, -d/--dbname) or by environmental
variables (PGHOST, PGPORT, PGUSER, PGDATABASE). If nothing is given, the default values are
taken (local connection, both database user name and database name are the same as operating user name).
Any database in the cluster can be specified to connect to.

To view the existing backups, run the command:
```
pg_probackup show
```

The following information is given:

* ID — the backup identifier. It is used for pointing to a specific backup in many commands.
* Recovery time — the least moment of time, the database cluster's state can be restored at.
* Mode — the method used to take this backup (FULL+STREAM — autonomous backup; other modes
are described below: FULL, PAGE, PTRACK).
* Current/Parent TLI — current and parent timelines of the database cluster.
* Time — time it took the backup to complete.
* Data — volume of data in this backup.
* Status — state of the backup (OK — the backup is created and ready for use,
ERROR — an error happened while the backup was being taken, CORRUPT — the backup is corrupted and cannot be used).

To get detailed information about the backup, specify its identifier in show command:
```
pg_probackup show backup_ID
```

To make sure a backup is correctly written to disk, pg\_probackup automatically checks its checksums immediately
after the backup was taken. A backup can be explicitly revalidated by running the following command:
```
pg_probackup validate backup_ID
```

To restore the database cluster from the backup, first stop the PostgerSQL service (if it is still running) and
then execute the following command:
```
pg_probackup restore backup_ID
```

After that start the database service. During startup, PostgreSQL will recover a self-consistent state by replaying
WAL files and be ready to accept connections.

Note that restoring from a backup can be performed exclusively by pg\_probackup utility.
Inside _backup\_directory/backups/backup\_ID/database/_ directory one can find files, corresponding to such in
cluster's data directory. Nevertheless there files cannot be copied directly into the data directory as
pg\_probackup always store them packed to save disk space.

### Continuous Archiving and Full Backups
[Continuous archiving](https://postgrespro.com/docs/postgresql/current/continuous-archiving.html) allows to restore
database cluster's state not only at the moment backup was taken, but at arbitrary point in time.
In most cases pg\_probackup is used along with continuous archiving.

Note that autonomous backups can still be useful:

* Autonomous backup can be restored on the server that for some reasons has no file access to WAL archive;
* To avoid running out of disk space in WAL archive, it should be periodically cleaned up.
An autonomous backup allows to restore cluster's state at some point in time, for which WAL
files are no longer available. (However one should prefer logical backups made by pg\_dumpall
for long-term storage, as it is possible that major release of PostgreSQL will change during that period.)

To enable continuous archiving on PostgreSQL server, configure the following parameters:

* [archive_mode](https://postgrespro.com/docs/postgresql/9.6/runtime-config-wal.html#guc-archive-mode) to 'on';
* [archive_command](https://postgrespro.com/docs/postgresql/9.6/runtime-config-wal.html#guc-archive-command) to 'test ! -f backup\_directory/wal/%f && cp %p backup\_directory/wal/%f'.

Utilities like rsync to copy WAL files over network are not currently supported; files must be accessible
in server's file system. To access files from a remote server, a network file system can be used.

To take a backup, execute the following command (specify additional connection options if needed):
```
pg_probackup backup -b full
```

The backup will only contain database cluster's files. WAL files necessary for recovery will be read
from archive in backup\_directory/wal/.

To restore the cluster from a backup, make sure that the database service is stopped and run the following command:
```
pg_probackup restore
```

The database cluster will be restored from the recent available backup and recovery.conf file will be created
to access the archived WAL files. When started, PostgreSQL server will automatically recover database cluster's
state using all available WAL files in the archive.

To restore the cluster's state at some arbitrary point in time, the following options (which correspond to
[recovery options](https://postgrespro.com/docs/postgresql/9.6/recovery-target-settings) in recovery.conf)
can be added:

* --timeline specifies recovering into a particular timeline;
* one of --time or --xid options specifies recovery target (either point in time or transaction id) up
to which recovery will proceed.
* --inclusive specifies whether to stop just after the specified recovery target, or just before it.

Closest to the specified recovery target backup will be automatically chosen for recovery.

All the described commands can operate autonomous backups the same way as full ones, using WAL
files either from the backup itself or from the archive.

A backup identifier can be specified right after the restore command to restore database cluster's
state at the moment shown in 'Recovery time' attribute of that backup:
```
pg_probackup restore backup_ID
```

For autonomous backups WAL archive will not be used. Full backups will use WAL archive only to
recover to self-consistent state.

When both backup identifier and one of --time or --xid options are specified for restore command,
recovery will start from the specified backup and will proceed up to the specified recovery target.
Usually there is no need in such mode, as backup identifier can be omitted to allow pg\_probackup to
choose it automatically.

### Incremental Backups

In addition to full backups pg\_probackup allows to take incremental backups, containing only the pages that have changed since the previous backup was taken. This way backups are smaller and may take less time to complete.

There are two modes for incremental backups: to track changes by scanning WAL files (PAGE), and to track changes on-the-fly (PTRACK).

In the first mode pg\_probackup scans all WAL files in archive starting from the moment the previous backup (either full or incremental) was taken. Newly created backup will contain only the pages that were mentioned in WAL records.

This way of operation requires all the WAL files since the previous backup to be present in the archive. In case the total size of these files is comparable to total size of database cluster's files, there will be no speedup (but still backup can be smaller by size).
```
pg_probackup backup -b page
```

The second mode (tracking changes on-the-fly) requires Postgres Pro server and will not work with PostgreSQL; continuous archiving is not necessary for it to operate.

When ptrack_enable parameter is on, Postgres Pro server tracks changed in data pages. Each time a WAL record for some relation's page is constructed, this page is marked in a special ptrack fork for this relation. As one page requires just one bit in the fork, the fork is quite small but significantly speeds up the process of taking a backup. Tracking implies some minor overhead for the database server.

While taking a backup (either full or incremental), pg_probackup clears ptrack fork of relations being processed. This ensures that the next incremental backup will contain only pages that have changed since the previous backup.
```
pg_probackup backup -b ptrack
```

If a backup resulted in an error (for example, was interrupted), some of relations probably have their ptrack forks already cleared. In this case next incremental backup will contain just part of all changes, which is useless. The same is true when ptrack\_enable parameter was turned on after the full backup was taken or when it was turned off for some time. Currently pg\_probackup does not verify that all changes for the increment were actually tracked. Fresh full backup should be taken before incremental ones in such circumstances.

To restore the database cluster from an incremental backup, pg_probackup first restores the full backup and then sequentially applies all the necessary increments. This is done automatically; restoration is managed exactly the same way as for full backups.

Incremental backup can be made autonomous by specifying --stream command line option. Such backup is autonomous only in regard to WAL archive: full backup and previous incremental backups are still needed to restore the cluster.

### Deleting of Backups

Unnecessary backup can be deleted by specifying its identifier in delete command:
```
pg_probackup delete backup_ID
```

This command will delete the specified backup along with all the following incremental backups, if any.

This way it is possible to delete some recent incremental backups, retaining an underlying full backup and some of incremental backups that follow it. In this case the next backup in PTRACK mode will not be correct as some changes since the last retained backup will be lost. Either full backup or incremental backup in PAGE mode (given that all necessary WAL files are still in the archive) should be taken then.

If --wal option is specified, WAL files not necessary to restore any of remaining backups will be deleted as well. This is a safe mode, because deletion of any backup will keep every possibly necessary WAL files.

To delete unnecessary WAL files without deleting any of backups, execute delwal command:
```
pg_probackup delwal
```

This command operates the same way as --wal option of delete command, except that it does not delete any backups.

Backup identifier can be specified in delwal command. In this case all WAL files will be deleted, except for those needed to restore from the specified backup and more recent backups.
```
pg_probackup delwal backup_ID
```

This mode should be used with caution as it allows to delete WAL files required for some of existing backups.

### Backup from Standby

If replication is in use, starting with PostgreSQL 9.6 a backup can be taken not only from primary server, but also from standby. Backup taken from standby is absolutely interchangeable with backup taken from primary (bearing in mind possible replication delay).

Currently it is required for primary database server to have full\_page\_writes turned on (in future this requirement may be relaxed in the case checksums are enabled on data pages).

The same backup directory can be used for pg\_probackup on both servers, primary and standby, as long as it is accessible in both server's file systems. This way all backups, taken from either primary or standby, are shown together and could be managed from one server or from the other.

A backup can be used to restore primary database server as well as standby. It depends on the server on which pg\_probackup is executed with restore command. Note that recovered PostgreSQL will always run as primary server if started right after the pg\_probackup. To run it as standby, edit recovery.conf file created by pg\_probackup: at least delete every parameter that specify recovery target (recovery\_target, recovery\_target\_time, and recovery\_target\_xid), change target timeline to 'latest', and add standby\_mode = 'on'. Probably primary\_conninfo should be added too for streaming replication, and hot\_standby = 'on' in database configuration parameters for hot standby mode.

### Backup Retention Policy

It is possible to configure the backup retention policy. The retention policy
specifies specifies which backups must be kept to meet data recoverability
requirements. The policy can be configured using two parameters: redundancy and
window.

Redundancy parameter specifies how many full backups purge command should keep.
For example, you make a full backup on Sunday and an incremental backup every day.
If redundancy is 1, then this backup will become obsolete on next Sunday and will
be deleted with all its incremental backups when next full backup will be created.

Window parameter specifies the number of days of data recoverability. If window
is 14, then all full backups older than 14 days will be deleted with their
incremental backups.

This parameters can be used together. Backups are obsolete if they don't
meet both parameters. For example, you have retention is 1, window is 14 and
two full backups are made 3 and 7 days ago. In this situation both backups aren't
obsolete and will be kept 14 days.

To delete obsolete backups execute the following command:
```
pg_probackup retention purge
```
Redundancy and window parameters values will be taken from command line options
or from pg_probackup.conf configuration file.

## Additional Features

### Parallel Execution
Backup, recovery, and validating process can be executed in several parallel threads. This can significantly speed up the operation given enough resources (CPU cores, disk and network throughput).

Parallel execution is specified by -j / --threads command line option, for example:
```
pg_probackup backup -b full -j 4
```

or
```
pg_probackup restore -j 4
```

Note that parallel recovery applies only to copying data from backup to cluster's data directory. When PostgreSQL server is started, it starts to replay WAL records (either from the archive or from local directory), and this currently cannot be paralleled.

### Checking Cluster and Backup Consistency

When checksums are enabled for the database cluster, pg\_probackup uses this information to check correctness of data files. While reading each page, pg_probackup checks whether calculated checksum coincides with the checksum stored in page. This guarantees that backup is free of corrupted pages; taking full backup effectively checks correctness of all cluster's data files.

Pages are packed before going to backup, leaving unused parts of pages behind (see database page layout). Hence the restored database cluster is not an exact copy of the original, but is binary-compatible with it.

Whether page checksums are enabled or not, pg\_probackup calculates checksums for each file in a backup. Checksums are checked immediately after backup is taken and right before restore, to timely detect possible backup corruptions.

## Options

Options for pg\_probackup utility can be specified in command line (such options are shown below starting from either one or two minus signs). If not given in command line, values for some options are derived from environmental variables (names of environmental variables are in uppercase). Otherwise values for some options are taken from pg\_probackup.conf configuration file, located in the backup directory (such option names are in lowercase).

### Common options:

-B _directory_  
--backup-path=_directory_  
BACKUP\_PATH

Absolute path to the backup directory. In this directory backups and WAL archive are stored.

-D _directory_  
--pgdata=directory _directory_  
PGDATA  
pgdata

Absolute path to database cluster's data directory.

-j _num\_threads_  
--threads=_num\_threads_

Number of parallel threads for backup, recovery, and backup validation.

--progress

Shows progress of operations.

-q  
--quiet

Do not write any messages.

-v  
--verbose

Show detailed messages.

--help

Show quick help on command line options.

--version

Show version information.

### Backup options:

-b _mode_  
--backup-mode=_mode_  
BACKUP\_MODE  
backup\_mode

Backup mode. Supported modes are: FULL (full backup), PAGE (incremental backup, tracking changes by scanning WAL files), PTRACK (incremental backup, tracking changes on-the-fly). The last mode requires Postgres Pro database server.

--stream

Makes an autonomous backup that includes all necessary WAL files, by streaming them from database server via replication protocol.

-S _slot\_name_  
--slot=_slot\_name_

This option causes the WAL streaming to use the specified replication slot, and is used together with --stream.

-C  
--smooth-checkpoint  
SMOOTH\_CHECKPOINT  
smooth\_checkpoint

Causes checkpoint to be spread out over a period of time (default is to complete checkpoint as soon as possible).

--backup-pg-log

Includes pg\_log directory (where logging is usually pointed to) in the backup. By default this directory is excluded.

Connection options for backup:

d db\_name  
--dbname=db\_name  
PGDATABASE

Specifies the name of the database to connect to (any one will do).

-h host  
--host=host  
PGHOST

Specifies the host name of the machine on which the server is running. If the value begins with a slash, it is used as the directory for the Unix-domain socket.

-p port  
--port=port  
PGPORT

Specifies the TCP port or local Unix domain socket file extension on which the server is listening for connections.

-U user\_name  
--username=user\_name  
PGUSER

User name to connect as.

-w  
--no-password

Never issue a password prompt. If the server requires password authentication and a password is not available by other means such as a .pgpass file, the connection attempt will fail. This option can be useful in batch jobs and scripts where no user is present to enter a password.

-W  
--password

Force pg\_probackup to prompt for a password before connecting to a database.

### Restore options:

--time

Specifies the timestamp up to which recovery will proceed.

--xid

Specifies the transaction ID up to which recovery will proceed.

--inclusive

Specifies whether to stop just after the specified recovery target (true), or just before the recovery target (false).

--timeline

Specifies recovering into a particular timeline.

-T
--tablespace-mapping=OLDDIR=NEWDIR

Relocate the tablespace in directory `OLDDIR` to `NEWDIR` during restore. Both
`OLDDIR` and `NEWDIR` must be absolute paths.

### Delete options:

--wal

Delete WAL files that are no longer necessary to restore from any of existing backups.

### Retention policy options:

--redundancy

Specifies how many full backups purge command should keep.

--window

Specifies the number of days of recoverability.

## Restrictions

Currently pg\_probackup has the following restrictions:

* The utility can be used only with PostgreSQL servers with the same major release and the same page size.
* PostgreSQL 9.5 or higher versions are supported.
* Windows operating system is not supported.
* Incremental backups in PTRACK mode can be taken only on Postgres Pro server.
* Data files from user tablespaces are restored to the same absolute paths as they were during backup.
* Configuration files outside PostgreSQL data directory are not included in backup and should be backed up separately.
* Only full backups are supported when using [compressed tablespaces](https://postgrespro.com/docs/postgresproee/current/cfs.html) (Postgres Pro Enterprise feature).

## Status Codes

On success pg\_probackup exits with 0 status.

Other values indicate an error (1 — generic error, 2 — repeated error, 3 — unexpected error).

## Authors

pg\_probackup utility is based on pg\_arman, that was originally written by NTT and then developed and maintained by Michael Paquier.

Features like parallel execution, incremental and autonomous backups are developed in Postgres Professional by Yury Zhuravlev (aka stalkerg).

Please report bugs and requests at https://github.com/postgrespro/pg\_probackup/issues .
