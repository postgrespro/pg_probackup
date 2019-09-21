# pg_probackup

pg_probackup is a utility to manage backup and recovery of PostgreSQL database clusters. It is designed to perform periodic backups of the PostgreSQL instance that enable you to restore the server in case of a failure. pg_probackup supports PostgreSQL 9.5 or higher.

Current version - 2.1.5

1. [Synopsis](#synopsis)
2. [Versioning](#versioning)
3. [Overview](#overview)
    * [Limitations](#limitations)

4. [Installation and Setup](#installation-and-setup)
    * [Initializing the Backup Catalog](#initializing-the-backup-catalog)
    * [Adding a New Backup Instance](#adding-a-new-backup-instance)
    * [Configuring the Database Cluster](#configuring-the-database-cluster)
    * [Setting up STREAM Backups](#setting-up-stream-backups)
    * [Setting up Continuous WAL Archiving](#setting-up-continuous-wal-archiving)
    * [Setting up Backup from Standby](#setting-up-backup-from-standby)
    * [Setting up Cluster Verification](#setting-up-cluster-verification)
    * [Setting up PTRACK Backups](#setting-up-ptrack-backups)
    * [Setting up Partial Restore](#setting-up-partial-restore)
    * [Configuring the Remote Mode](#configuring-the-remote-mode)

5. [Usage](#usage)
    * [Creating a Backup](#creating-a-backup)
        * [ARCHIVE WAL mode](#archive-mode)
        * [STREAM WAL mode](#stream-mode)
        * [Page validation](#page-validation)
        * [External directories](#external-directories)
    * [Verifying a Cluster](#verifying-a-cluster)
    * [Validating a Backup](#validating-a-backup)
    * [Restoring a Cluster](#restoring-a-cluster)
        * [Partial Restore](#partial-restore)
    * [Performing Point-in-Time (PITR) Recovery](#performing-point-in-time-pitr-recovery)
    * [Using pg_probackup in the Remote Mode](#using-pg_probackup-in-the-remote-mode)
    * [Running pg_probackup on Parallel Threads](#running-pg_probackup-on-parallel-threads)
    * [Configuring pg_probackup](#configuring-pg_probackup)
    * [Managing the Backup Catalog](#managing-the-backup-catalog)
        * [Viewing WAL Archive Information](#viewing-wal-archive-information)
    * [Configuring Backup Retention Policy](#configuring-backup-retention-policy)
    * [Merging Backups](#merging-backups)
    * [Deleting Backups](#deleting-backups)

6. [Command-Line Reference](#command-line-reference)
    * [Commands](#commands)
        * [version](#version)
        * [help](#help)
        * [init](#init)
        * [add-instance](#add-instance)
        * [del-instance](#del-instance)
        * [set-config](#set-config)
        * [show-config](#show-config)
        * [show](#show)
        * [backup](#backup)
        * [restore](#restore)
        * [checkdb](#checkdb)
        * [validate](#validate)
        * [merge](#merge)
        * [delete](#delete)
        * [archive-push](#archive-push)
        * [archive-get](#archive-get)
    * [Options](#options)
        * [Common Options](#common-options)
        * [Recovery Target Options](#recovery-target-options)
        * [Retention Options](#retention-options)
        * [Logging Options](#logging-options)
        * [Connection Options](#connection-options)
        * [Compression Options](#compression-options)
        * [Archiving Options](#archiving-options)
        * [Remote Mode Options](#remote-mode-options)
        * [Remote WAL Archive Options](#remote-wal-archive-options)
        * [Replica Options](#replica-options)

7. [Authors](#authors)
8. [Credits](#credits)


## Synopsis

`pg_probackup version`

`pg_probackup help [command]`

`pg_probackup init -B backup_dir`

`pg_probackup add-instance -B backup_dir -D data_dir --instance instance_name`

`pg_probackup del-instance -B backup_dir --instance instance_name`

`pg_probackup set-config -B backup_dir --instance instance_name [option...]`

`pg_probackup show-config -B backup_dir --instance instance_name [--format=format]`

`pg_probackup show -B backup_dir [option...]`

`pg_probackup backup -B backup_dir --instance instance_name -b backup_mode [option...]`

`pg_probackup restore -B backup_dir --instance instance_name [option...]`

`pg_probackup checkdb -B backup_dir --instance instance_name [-D data_dir] [option...]`

`pg_probackup validate -B backup_dir [option...]`

`pg_probackup merge -B backup_dir --instance instance_name -i backup_id [option...]`

`pg_probackup delete -B backup_dir --instance instance_name { -i backup_id | --delete-wal | --delete-expired | --merge-expired }`

`pg_probackup archive-push -B backup_dir --instance instance_name --wal-file-path=wal_file_path --wal-file-name=wal_file_name [option...]`

`pg_probackup archive-get -B backup_dir --instance instance_name --wal-file-path=wal_file_path --wal-file-name=wal_file_name`


## Versioning

pg_probackup is following the [semantic](https://semver.org/) versioning.

## Overview

As compared to other backup solutions, pg_probackup offers the following benefits that can help you implement different backup strategies and deal with large amounts of data:

- Incremental backup: page-level incremental backup allows you to save disk space, speed up backup and restore. With three different incremental modes you can plan the backup strategy in accordance with your data flow
- Validation: automatic data consistency checks and on-demand backup validation without actual data recovery
- Verification: on-demand verification of PostgreSQL instance via dedicated command `checkdb`
- Retention: managing backups in accordance with retention policies - Time and/or Redundancy based, with two retention methods: `delete expired` and `merge expired`
- Parallelization: running backup, restore, merge, delete, verificaton and validation processes on multiple parallel threads
- Compression: storing backup data in a compressed state to save disk space
- Deduplication: saving disk space by not copying the not changed non-data files ('_vm', '_fsm', etc)
- Remote operations: backup PostgreSQL instance located on remote machine or restore backup on it
- Backup from replica: avoid extra load on the master server by taking backups from a standby
- External directories: add to backup content of directories located outside of the PostgreSQL data directory (PGDATA), such as scripts, configs, logs and pg_dump files
- Backup Catalog: get list of backups and corresponding meta information in `plain` or `json` formats and view WAL Archive information.
- Partial Restore: restore the only specified databases or skip the specified databases.

To manage backup data, pg_probackup creates a `backup catalog`. This is a directory that stores all backup files with additional meta information, as well as WAL archives required for point-in-time recovery. You can store backups for different instances in separate subdirectories of a single backup catalog.

Using pg_probackup, you can take full or incremental [backups](#creating-a-backup):

- FULL backups contain all the data files required to restore the database cluster.
- Incremental backups only store the data that has changed since the previous backup. It allows to decrease the backup size and speed up backup and restore operations. pg_probackup supports the following modes of incremental backups:
    - DELTA backup. In this mode, pg_probackup reads all data files in the data directory and copies only those pages that has changed since the previous backup. Note that this mode can impose read-only I/O pressure equal to a full backup.
    - PAGE backup. In this mode, pg_probackup scans all WAL files in the archive from the moment the previous full or incremental backup was taken. Newly created backups contain only the pages that were mentioned in WAL records. This requires all the WAL files since the previous backup to be present in the WAL archive. If the size of these files is comparable to the total size of the database cluster files, speedup is smaller, but the backup still takes less space. You have to configure WAL archiving as explained in the section [Setting up continuous WAL archiving](#setting-up-continuous-wal-archiving) to make PAGE backups.
    - PTRACK backup. In this mode, PostgreSQL tracks page changes on the fly. Continuous archiving is not necessary for it to operate. Each time a relation page is updated, this page is marked in a special PTRACK bitmap for this relation. As one page requires just one bit in the PTRACK fork, such bitmaps are quite small. Tracking implies some minor overhead on the database server operation, but speeds up incremental backups significantly.

pg_probackup can take only physical online backups, and online backups require WAL for consistent recovery. So regardless of the chosen backup mode (FULL, PAGE or DELTA), any backup taken with pg_probackup must use one of the following `WAL delivery modes`:

- [ARCHIVE](#archive-mode). Such backups rely on [continuous archiving](#setting-up-continuous-wal-archiving) to ensure consistent recovery. This is the default WAL delivery mode.
- [STREAM](#stream-mode). Such backups include all the files required to restore the cluster to a consistent state at the time the backup was taken. Regardless of [continuous archiving](#setting-up-continuous-wal-archiving) been set up or not, the WAL segments required for consistent recovery are streamed (hence STREAM) via replication protocol during backup and included into the backup files. Because of that backups of this WAL mode are called `autonomous` or `standalone`.

### Limitations

pg_probackup currently has the following limitations:

- Only PostgreSQL of versions 9.5 and newer are supported.
- Currently remode mode of operations is not supported on Windows systems.
- On Unix systems backup of PostgreSQL verions =< 10 is possible only by the same OS user PostgreSQL server is running by. For example, if PostgreSQL server is running by user *postgres*, then backup must be run by user *postgres*. If backup is running in [remote mode](#using-pg_probackup-in-the-remote-mode) using `ssh`, then this limitation apply differently: value for `--remote-user` option should be *postgres*.
- During backup of PostgreSQL 9.5 functions `pg_create_restore_point(text)` and `pg_switch_xlog()` will be executed only if backup role is superuser. Because of that backup of a cluster with low amount of WAL traffic with non-superuser role may take more time than backup of the same cluster with superuser role.
- The PostgreSQL server from which the backup was taken and the restored server must be compatible by the [block_size](https://www.postgresql.org/docs/current/runtime-config-preset.html#GUC-BLOCK-SIZE) and [wal_block_size](https://www.postgresql.org/docs/current/runtime-config-preset.html#GUC-WAL-BLOCK-SIZE) parameters and have the same major release number. Also depending on cluster configuration PostgreSQL itself may apply additional restrictions such as CPU architecture platform and libc/libicu versions.
- Incremental chain can span only within one timeline. So if you have backup incremental chain taken from replica and it gets promoted, you would be forced to take another FULL backup.

## Installation and Setup

Once you have pg_probackup installed, complete the following setup:

- Initialize the backup catalog.
- Add a new backup instance to the backup catalog.
- Configure the database cluster to enable pg_probackup backups.
- Optionally, configure SSH for running pg_probackup operations in remote mode.

### Initializing the Backup Catalog

pg_probackup stores all WAL and backup files in the corresponding subdirectories of the backup catalog.

To initialize the backup catalog, run the following command:

    pg_probackup init -B backup_dir

Where *backup_dir* is the path to backup catalog. If the *backup_dir* already exists, it must be empty. Otherwise, pg_probackup returns an error.

The user launching pg_probackup must have full access to *backup_dir* directory.

pg_probackup creates the backup_dir backup catalog, with the following subdirectories:

- wal/ — directory for WAL files.
- backups/ — directory for backup files.

Once the backup catalog is initialized, you can add a new backup instance.

### Adding a New Backup Instance

pg_probackup can store backups for multiple database clusters in a single backup catalog. To set up the required subdirectories, you must add a backup instance to the backup catalog for each database cluster you are going to back up.

To add a new backup instance, run the following command:

    pg_probackup add-instance -B backup_dir -D data_dir --instance instance_name [remote_options]

Where:

- *data_dir* is the data directory of the cluster you are going to back up. To set up and use pg_probackup, write access to this directory is required.
- *instance_name* is the name of the subdirectories that will store WAL and backup files for this cluster.
- The optional parameters [remote_options](#remote-mode-options) should be used if *data_dir* is located on remote machine.

pg_probackup creates the *instance_name* subdirectories under the 'backups/' and 'wal/' directories of the backup catalog. The 'backups/*instance_name*' directory contains the 'pg_probackup.conf' configuration file that controls pg_probackup settings for this backup instance. If you run this command with the [remote_options](#remote-mode-options), used parameters will be added to pg_probackup.conf.

For details on how to fine-tune pg_probackup configuration, see the section [Configuring pg_probackup](#configuring-pg_probackup).

The user launching pg_probackup must have full access to *backup_dir* directory and at least read-only access to *data_dir* directory. If you specify the path to the backup catalog in the `BACKUP_PATH` environment variable, you can omit the corresponding option when running pg_probackup commands.

>NOTE: For PostgreSQL >= 11 it is recommended to use [allow-group-access](https://www.postgresql.org/docs/11/app-initdb.html#APP-INITDB-ALLOW-GROUP-ACCESS) feature, so backup can be done by OS user with read-only permissions.

### Configuring the Database Cluster

Although pg_probackup can be used by a superuser, it is recommended to create a separate role with the minimum permissions required for the chosen backup strategy. In these configuration instructions, the *backup* role is used as an example.

To perform [backup](#backup), the following permissions for role *backup* are required only in database **used for connection** to PostgreSQL server:

For PostgreSQL 9.5:
```
BEGIN;
CREATE ROLE backup WITH LOGIN;
GRANT USAGE ON SCHEMA pg_catalog TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean) TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_xlog() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.txid_current() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;
COMMIT;
```

For PostgreSQL 9.6:
```
BEGIN;
CREATE ROLE backup WITH LOGIN;
GRANT USAGE ON SCHEMA pg_catalog TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean, boolean) TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup(boolean) TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_xlog() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_xlog_replay_location() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.txid_current() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;
COMMIT;
```

For PostgreSQL >= 10:
```
BEGIN;
CREATE ROLE backup WITH LOGIN;
GRANT USAGE ON SCHEMA pg_catalog TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.current_setting(text) TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_start_backup(text, boolean, boolean) TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_stop_backup(boolean, boolean) TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_create_restore_point(text) TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_last_wal_replay_lsn() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.txid_current() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.txid_current_snapshot() TO backup;
GRANT EXECUTE ON FUNCTION pg_catalog.txid_snapshot_xmax(txid_snapshot) TO backup;
COMMIT;
```

In the [pg_hba.conf](https://www.postgresql.org/docs/current/auth-pg-hba-conf.html) file, allow connection to database cluster on behalf of the *backup* role.

Since pg_probackup needs to read cluster files directly, pg_probackup must be started by (in case of remote backup - connected to) OS user that has read access to all files and directories inside the data directory (PGDATA) you are going to back up.

Depending on whether you are plan to take [autonomous](#stream-mode) and/or [archive](#archive-mode) backups, PostgreSQL cluster configuration will differ, as specified in the sections below. To back up the database cluster from a standby server, run pg_probackup in remote mode or create PTRACK backups, additional setup is required.

For details, see the sections [Setting up STREAM Backups](#setting-up-stream-backups), [Setting up continuous WAL archiving](#setting-up-continuous-wal-archiving), [Setting up Backup from Standby](#setting-up-backup-from-standby), [Configuring the Remote Mode](#configuring-the-remote-mode) and [Setting up PTRACK Backups](#setting-up-ptrack-backups).

### Setting up STREAM Backups

To set up the cluster for [STREAM](#stream-mode) backups, complete the following steps:

- Grant the REPLICATION privilege to the backup role:

        ALTER ROLE backup WITH REPLICATION;

- In the [pg_hba.conf](https://www.postgresql.org/docs/current/auth-pg-hba-conf.html) file, allow replication on behalf of the *backup* role.
- Make sure the parameter [max_wal_senders](https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-MAX-WAL-SENDERS) is set high enough to leave at least one session available for the backup process.
- Set the parameter [wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-WAL-LEVEL) to be higher than `minimal`.

If you are planning to take PAGE backups in STREAM mode or perform PITR with STREAM backups, you still have to configure WAL archiving as explained in the section [Setting up continuous WAL archiving](#setting-up-continuous-wal-archiving).

Once these steps are complete, you can start taking FULL, PAGE, DELTA and PTRACK backups with [STREAM](#stream-mode) WAL mode.

### Setting up continuous WAL archiving

Making backups in PAGE backup mode, performing [PITR](#performing-point-in-time-pitr-recovery) and making backups with [ARCHIVE](#archive-mode) WAL delivery mode require [continuous WAL archiving](https://www.postgresql.org/docs/current/continuous-archiving.html) to be enabled. To set up continuous archiving in the cluster, complete the following steps:

- Make sure the [wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-WAL-LEVEL) parameter is higher than `minimal`.
- If you are configuring archiving on master, [archive_mode](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-ARCHIVE-MODE) must be set to `on` or `always`. To perform archiving on standby, set this parameter to `always`.
- Set the [archive_command](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-ARCHIVE-COMMAND) parameter, as follows:

        archive_command = 'pg_probackup archive-push -B backup_dir --instance instance_name --wal-file-path=%p --wal-file-name=%f [remote_options]'

Where *backup_dir* and *instance_name* refer to the already initialized backup catalog instance for this database cluster and optional parameters [remote_options](#remote-mode-options) should be used to archive WAL to the remote host. For details about all possible `archive-push` parameters, see the section [archive-push](#archive-push).

Once these steps are complete, you can start making backups with [ARCHIVE](#archive-mode) WAL-mode, backups in PAGE backup mode and perform [PITR](#performing-point-in-time-pitr-recovery).

Current state of WAL Archive can be obtained via [show](#show) command. For details, see the sections [Viewing WAL Archive information](#viewing-wal-archive-information).

If you are planning to make PAGE backups and/or backups with [ARCHIVE](#archive-mode) WAL mode from a standby of a server, that generates small amount of WAL traffic, without long waiting for WAL segment to fill up, consider setting [archive_timeout](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-ARCHIVE-TIMEOUT) PostgreSQL parameter **on master**. It is advisable to set the value of this setting slightly lower than pg_probackup parameter `--archive-timeout` (default 5 min), so there should be enough time for rotated segment to be streamed to replica and send to archive before backup is aborted because of `--archive-timeout`.

>NOTE: using pg_probackup command [archive-push](#archive-push) for continuous archiving is optional. You can use any other tool you like as long as it delivers WAL segments into '*backup_dir*/wal/*instance_name*' directory. If compression is used, it should be `gzip`, and '.gz' suffix in filename is mandatory.

>NOTE: Instead of `archive_mode`+`archive_command` method you may opt to use the utility [pg_receivewal](https://www.postgresql.org/docs/current/app-pgreceivewal.html). In this case pg_receivewal `-D directory` option should point to '*backup_dir*/wal/*instance_name*' directory. WAL compression that could be done by pg_receivewal is supported by pg_probackup. `Zero Data Loss` archive strategy can be achieved only by using pg_receivewal.

### Setting up Backup from Standby

For PostgreSQL 9.6 or higher, pg_probackup can take backups from a standby server. This requires the following additional setup:

- On the standby server, set the parameter [hot_standby](https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-HOT-STANDBY) to `on`.
- On the master server, set the parameter [full_page_writes](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-FULL-PAGE-WRITES) to `on`.
- To perform autonomous backups on standby, complete all steps in section [Setting up STREAM Backups](#setting-up-stream-backups)
- To perform archive backups on standby, complete all steps in section [Setting up continuous WAL archiving](#setting-up-continuous-wal-archiving)

Once these steps are complete, you can start taking FULL, PAGE, DELTA or PTRACK backups with appropriate WAL delivery mode: ARCHIVE or STREAM, from the standby server.

Backup from the standby server has the following limitations:

- If the standby is promoted to the master during backup, the backup fails.
- All WAL records required for the backup must contain sufficient full-page writes. This requires you to enable `full_page_writes` on the master, and not to use a tools like pg_compresslog as [archive_command](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-ARCHIVE-COMMAND) to remove full-page writes from WAL files.

### Setting up Cluster Verification

Logical verification of database cluster requires the following additional setup. Role *backup* is used as an example:

- Install extension [amcheck](https://www.postgresql.org/docs/current/amcheck.html) or [amcheck_next](https://github.com/petergeoghegan/amcheck) **in every database** of the cluster:

        CREATE EXTENSION amcheck;

- To perform logical verification the following permissions are required **in every database** of the cluster:

```
GRANT SELECT ON TABLE pg_catalog.pg_am TO backup;
GRANT SELECT ON TABLE pg_catalog.pg_class TO backup;
GRANT SELECT ON TABLE pg_catalog.pg_database TO backup;
GRANT SELECT ON TABLE pg_catalog.pg_namespace TO backup;
GRANT SELECT ON TABLE pg_catalog.pg_extension TO backup;
GRANT EXECUTE ON FUNCTION bt_index_check(oid) TO backup;
GRANT EXECUTE ON FUNCTION bt_index_check(oid, bool) TO backup;
```

### Setting up PTRACK Backups

Backup mode PTACK can be used only on Postgrespro Standart and Postgrespro Enterprise installations or patched vanilla PostgreSQL. Links to ptrack patches can be found [here](https://github.com/postgrespro/pg_probackup#ptrack-support).

If you are going to use PTRACK backups, complete the following additional steps:

- Set the parameter `ptrack_enable` to `on`.
- Grant the rights to execute `ptrack` functions to the *backup* role **in every database** of the cluster:

        GRANT EXECUTE ON FUNCTION pg_catalog.pg_ptrack_clear() TO backup;
        GRANT EXECUTE ON FUNCTION pg_catalog.pg_ptrack_get_and_clear(oid, oid) TO backup;

- The *backup* role must have access to all the databases of the cluster.

### Configuring the Remote Mode

pg_probackup supports the remote mode that allows to perform backup, restore and WAL archiving operations remotely. In this mode, the backup catalog is stored on a local system, while PostgreSQL instance to backup and/or to restore is located on a remote system. Currently the only supported remote protocol is SSH.

#### Setup SSH

If you are going to use pg_probackup in remote mode via ssh, complete the following steps:

- Install pg_probackup on both systems: `backup_host` and `db_host`.
- For communication between the hosts setup the passwordless SSH connection between *backup* user on `backup_host` and *postgres* user on `db_host`:

    [backup@backup_host] ssh-copy-id postgres@db_host

- If you are planning to rely on [continuous WAL archiving](#setting-up-continuous-wal-archiving), then setup passwordless SSH connection between *postgres* user on `db_host` and *backup* user on `backup_host`:

    [postgres@db_host] ssh-copy-id backup@backup_host

Where:

- *backup_host* is the system with *backup catalog*.
- *db_host* is the system with PostgreSQL cluster.
- *backup* is the OS user on *backup_host* used to run pg_probackup.
- *postgres* is the OS user on *db_host* used to run PostgreSQL cluster. Note, that for PostgreSQL versions >= 11, a more secure approach can used thanks to [allow-group-access](https://www.postgresql.org/docs/11/app-initdb.html#APP-INITDB-ALLOW-GROUP-ACCESS) feature.

pg_probackup in remote mode via `ssh` works as follows:

- only the following commands can be launched in remote mode: [add-instance](#add-instance), [backup](#backup), [restore](#restore), [archive-push](#archive-push), [archive-get](#archive-get).
- when started in remote mode the main pg_probackup process on local system connects via ssh to remote system and launches there number of agent proccesses equal to specified value of option `-j/--threads`.
- the main pg_probackup process use remote agents to access remote files and transfer data between local and remote systems.
- remote agents are smart and capable of handling some logic on their own to minimize the network traffic and number of round-trips between hosts.
- usually the main proccess is started on *backup_host* and connects to *db_host*, but in case of `archive-push` and `archive-get` commands the main process is started on *db_host* and connects to *backup_host*.
- after completition of data transfer the remote agents are terminated and ssh connections are closed.
- if an error condition is encountered by a remote agent, then all agents are terminated and error details are reported by the main pg_probackup process, which exits with error.
- compression is always done on *db_host*.
- decompression is always done on *backup_host*.

>NOTE: You can improse [additional restrictions](https://man.openbsd.org/OpenBSD-current/man8/sshd.8#AUTHORIZED_KEYS_FILE_FORMAT) on ssh settings to protect the system in the event of account compromise.

## Usage

### Creating a Backup

To create a backup, run the following command:

    pg_probackup backup -B backup_dir --instance instance_name -b backup_mode

Where *backup_mode* can take one of the following values:

- FULL — creates a full backup that contains all the data files of the cluster to be restored.
- DELTA — reads all data files in the data directory and creates an incremental backup for pages that have changed since the previous backup.
- PAGE — creates an incremental PAGE backup based on the WAL files that have generated since the previous full or incremental backup was taken. Only changed blocks are readed from data files.
- PTRACK — creates an incremental PTRACK backup tracking page changes on the fly.

When restoring a cluster from an incremental backup, pg_probackup relies on the parent full backup and all the incremental backups between them, which is called `the backup chain`. You must create at least one full backup before taking incremental ones.

#### ARCHIVE mode

ARCHIVE is the default WAL delivery mode.

For example, to make a FULL backup in ARCHIVE mode, run:

    pg_probackup backup -B backup_dir --instance instance_name -b FULL

Unlike backup in STREAM mode, ARCHIVE backup rely on [continuous archiving](#setting-up-continuous-wal-archiving) to provide WAL segments required to restore the cluster to a consistent state at the time the backup was taken.

During [backup](#backup) pg_probackup ensures that WAL files containing WAL records between START LSN and STOP LSN are actually exists in '*backup_dir*/wal/*instance_name*' directory. Also pg_probackup ensures that WAL records between START LSN and STOP LSN can be parsed. This precations eliminates the risk of silent WAL corruption.

#### STREAM mode

STREAM is the optional WAL delivery mode.

For example, to make a FULL backup in STREAM mode, add the `--stream` flag to the command from the previous example:

    pg_probackup backup -B backup_dir --instance instance_name -b FULL --stream --temp-slot

The optional `--temp-slot` flag ensures that the required segments remain available if the WAL is rotated before the backup is complete.

Unlike backup in ARCHIVE mode, STREAM backup include all the WAL segments required to restore the cluster to a consistent state at the time the backup was taken.

During [backup](#backup) pg_probackup streams WAL files containing WAL records between START LSN and STOP LSN to '*backup_dir*/backups/*instance_name*/*BACKUP ID*/database/pg_wal' directory. Also pg_probackup ensures that WAL records between START LSN and STOP LSN can be parsed. This precations eliminates the risk of silent WAL corruption.

Even if you are using [continuous archiving](#setting-up-continuous-wal-archiving), STREAM backups can still be useful in the following cases:

- STREAM backups can be restored on the server that has no file access to WAL archive.
- STREAM backups enable you to restore the cluster state at the point in time for which WAL files in archive are no longer available.
- Backup in STREAM mode can be taken from standby of a server, that generates small amount of WAL traffic, without long waiting for WAL segment to fill up.

#### Page validation

If [data checksums](https://www.postgresql.org/docs/current/runtime-config-preset.html#GUC-DATA-CHECKSUMS) are enabled in the database cluster, pg_probackup uses this information to check correctness of data files during backup. While reading each page, pg_probackup checks whether the calculated checksum coincides with the checksum stored in the page header. This guarantees that the PostgreSQL instance and backup itself are free of corrupted pages.
Note that pg_probackup reads database files directly from filesystem, so under heavy write load during backup it can show false positive checksum failures because of partial writes. In case of page checksumm mismatch, page is readed again and checksumm comparison repeated.

Page is considered corrupted if checksumm comparison failed more than 100 times, in this case backup is aborted.

Redardless of data checksums been enabled or not, pg_probackup always check page header "sanity".

#### External directories

To back up a directory located outside of the data directory, use the optional `--external-dirs` parameter that specifies the path to this directory. If you would like to add more than one external directory, provide several paths separated by colons, on Windows system paths must be separated by semicolon instead.

For example, to include `'/etc/dir1/'` and `'/etc/dir2/'` directories into the full backup of your *instance_name* instance that will be stored under the *backup_dir* directory, run:

    pg_probackup backup -B backup_dir --instance instance_name -b FULL --external-dirs=/etc/dir1:/etc/dir2

For example, to include `'C:\dir1\'` and `'C:\dir2\'` directories into the full backup of your *instance_name* instance that will be stored under the *backup_dir* directory on Windows system, run:

    pg_probackup backup -B backup_dir --instance instance_name -b FULL --external-dirs=C:\dir1;C:\dir2

pg_probackup creates a separate subdirectory in the backup directory for each external directory. Since external directories included into different backups do not have to be the same, when you are restoring the cluster from an incremental backup, only those directories that belong to this particular backup will be restored. Any external directories stored in the previous backups will be ignored.

To include the same directories into each backup of your instance, you can specify them in the pg_probackup.conf configuration file using the [set-config](#set-config) command with the `--external-dirs` option.

### Verifying a Cluster

To verify that PostgreSQL database cluster is free of corruption, run the following command:

    pg_probackup checkdb [-B backup_dir [--instance instance_name]] [-D data_dir]

This physical verification works similar to [page validation](#page-validation) that is done during backup with several differences:

- `checkdb` is read-only
- if corrupted page is detected, `checkdb` is not aborted, but carry on, until all pages in the cluster are validated
- `checkdb` do not strictly require *the backup catalog*, so it can be used to verify database clusters that are **not** [added to the backup catalog](#adding-a-new-backup-instance).

If *backup_dir* and *instance_name* are omitted, then [connection options](#connection-options) and *data_dir* must be provided via environment variables or command-line options.

Physical verification cannot detect logical inconsistencies, missing and nullified blocks or entire files, repercussions from PostgreSQL bugs and other wicked anomalies.
Extensions [amcheck](https://www.postgresql.org/docs/current/amcheck.html) and [amcheck_next](https://github.com/petergeoghegan/amcheck) provide a partial solution to these problems.

If you would like, in addition to physical verification, to verify all indexes in all databases using these extensions, you can specify `--amcheck` flag when running [checkdb](#checkdb) command:

    pg_probackup checkdb -D data_dir --amcheck

Physical verification can be skipped if `--skip-block-validation` flag is used. For logical only verification *backup_dir* and *data_dir* are optional, only [connection options](#connection-options) are mandatory:

    pg_probackup checkdb --amcheck --skip-block-validation {connection_options}

Logical verification can be done more thoroughly with flag `--heapallindexed` by checking that all heap tuples that should be indexed are actually indexed, but at the higher cost of CPU, memory and I/O comsumption.

### Validating a Backup

pg_probackup calculates checksums for each file in a backup during backup process. The process of checking  checksumms of backup data files is called `the backup validation`. By default validation is run immediately after backup is taken and right before restore, to detect possible backup corruption.

If you would like to skip backup validation, you can specify the `--no-validate` flag when running [backup](#backup) and [restore](#restore) commands.

To ensure that all the required backup files are present and can be used to restore the database cluster, you can run the [validate](#validate) command with the exact [recovery target options](#recovery-target-options) you are going to use for recovery.

For example, to check that you can restore the database cluster from a backup copy up to the specified xid transaction ID, run this command:

    pg_probackup validate -B backup_dir --instance instance_name --recovery-target-xid=4242

If validation completes successfully, pg_probackup displays the corresponding message. If validation fails, you will receive an error message with the exact time, transaction ID and LSN up to which the recovery is possible.

If you specify *backup_id* via `-i/--backup-id` option, then only backup copy with specified backup ID will be validated. If *backup_id* is specified with [recovery target options](#recovery-target-options) then validate will check whether it is possible to restore the specified backup to the specified `recovery target`.

For example, to check that you can restore the database cluster from a backup copy with *backup_id* up to the specified timestamp, run this command:

    pg_probackup validate -B backup_dir --instance instance_name -i PT8XFX --recovery-target-time='2017-05-18 14:18:11+03'

If *backup_id* belong to incremental backup, then all its parents starting from FULL backup will be validated.

If you omit all the parameters, all backups are validated.

### Restoring a Cluster

To restore the database cluster from a backup, run the restore command with at least the following options:

    pg_probackup restore -B backup_dir --instance instance_name -i backup_id

Where:

- *backup_dir* is the backup catalog that stores all backup files and meta information.
- *instance_name* is the backup instance for the cluster to be restored.
- *backup_id* specifies the backup to restore the cluster from. If you omit this option, pg_probackup uses the latest valid backup available for the specified instance. If you specify an incremental backup to restore, pg_probackup automatically restores the underlying full backup and then sequentially applies all the necessary increments.

If the cluster to restore contains tablespaces, pg_probackup restores them to their original location by default. To restore tablespaces to a different location, use the `--tablespace-mapping/-T` option. Otherwise, restoring the cluster on the same host will fail if tablespaces are in use, because the backup would have to be written to the same directories.

When using the `--tablespace-mapping/-T` option, you must provide absolute paths to the old and new tablespace directories. If a path happens to contain an equals sign (=), escape it with a backslash. This option can be specified multiple times for multiple tablespaces. For example:

    pg_probackup restore -B backup_dir --instance instance_name -D data_dir -j 4 -i backup_id -T tablespace1_dir=tablespace1_newdir -T tablespace2_dir=tablespace2_newdir

Once the restore command is complete, start the database service.

If you are restoring an STREAM backup, the restore is complete at once, with the cluster returned to a self-consistent state at the point when the backup was taken. For ARCHIVE backups, PostgreSQL replays all available archived WAL segments, so the cluster is restored to the latest state possible. You can change this behavior by using the [recovery target options](#recovery-target-options) with the `restore` command. Note that using the [recovery target options](#recovery-target-options) when restoring STREAM backup is possible if the WAL archive is available at least starting from the time the STREAM backup was taken.

>NOTE: By default, the [restore](#restore) command validates the specified backup before restoring the cluster. If you run regular backup validations and would like to save time when restoring the cluster, you can specify the `--no-validate` flag to skip validation and speed up the recovery.

#### Partial Restore

If you have enabled [partial restore](#setting-up-partial-restore) before taking backups, you can restore or exclude from restore the arbitraty number of specific databases using [partial restore options](#partial-restore-options) with the [restore](#restore) commands.

To restore only one or more databases, run the restore command with the following options:

    pg_probackup restore -B backup_dir --instance instance_name --db-include=database_name

The option `--db-include` can be specified multiple times. For example, to restore only databases `db1` and `db2`, run the following command:

    pg_probackup restore -B backup_dir --instance instance_name --db-include=db1 --db-include=db2

To exclude one or more specific databases from restore, run the following options:

    pg_probackup restore -B backup_dir --instance instance_name --db-exclude=database_name

The option `--db-exclude` can be specified multiple times. For example, to exclude the databases `db1` and `db2` from restore, run the following command:

    pg_probackup restore -B backup_dir --instance instance_name -i backup_id --db-exclude=db1 --db-exclude=db2

Partial restore rely on lax behaviour of PostgreSQL recovery process toward truncated files. Files of excluded databases restored as null sized files, allowing recovery to work properly. After successfull starting of PostgreSQL cluster, you must drop excluded databases using `DROP DATABASE` command.

>NOTE: The databases `template0` and `template1` are always restored.

### Performing Point-in-Time (PITR) Recovery

If you have enabled [continuous WAL archiving](#setting-up-continuous-wal-archiving) before taking backups, you can restore the cluster to its state at an arbitrary point in time (recovery target) using [recovery target options](#recovery-target-options) with the [restore](#restore) and [validate](#validate) commands.

If `-i/--backup-id` option is omitted, pg_probackup automatically chooses the backup that is the closest to the specified recovery target and starts the restore process, otherwise pg_probackup will try to restore *backup_id* to the specified recovery target.

- To restore the cluster state at the exact time, specify the `--recovery-target-time` option, in the timestamp format. For example:

        pg_probackup restore -B backup_dir --instance instance_name --recovery-target-time='2017-05-18 14:18:11+03'

- To restore the cluster state up to a specific transaction ID, use the `--recovery-target-xid` option:

        pg_probackup restore -B backup_dir --instance instance_name --recovery-target-xid=687

- To restore the cluster state up to a specific LSN, use `--recovery-target-lsn` option:

        pg_probackup restore -B backup_dir --instance instance_name --recovery-target-lsn=16/B374D848

- To restore the cluster state up to a specific named restore point, use `--recovery-target-name` option:

        pg_probackup restore -B backup_dir --instance instance_name --recovery-target-name='before_app_upgrade'

- To restore the backup to the latest state available in archive, use `--recovery-target` option with `latest` value:

        pg_probackup restore -B backup_dir --instance instance_name --recovery-target='latest'

- To restore the cluster to the earliest point of consistency, use `--recovery-target` option with `immediate` value:

        pg_probackup restore -B backup_dir --instance instance_name --recovery-target='immediate'

### Using pg_probackup in the Remote Mode

pg_probackup supports the remote mode that allows to perform `backup` and `restore` operations remotely via SSH. In this mode, the backup catalog is stored on a local system, while PostgreSQL instance to be backed up is located on a remote system. You must have pg_probackup installed on both systems.

Do note that pg_probackup rely on passwordless SSH connection for communication between the hosts.

The typical workflow is as follows:

 - On your backup host, configure pg_probackup as explained in the section [Installation and Setup](#installation-and-setup). For the [add-instance](#add-instance) and [set-config](#set-config) commands, make sure to specify [remote options](#remote-mode-options) that point to the database host with the PostgreSQL instance.

- If you would like to take remote backup in [PAGE](#creating-a-backup) mode, or rely on [ARCHIVE](#archive-mode) WAL delivery mode, or use [PITR](#performing-point-in-time-pitr-recovery), then configure continuous WAL archiving from database host to the backup host as explained in the section [Setting up continuous WAL archiving](#setting-up-continuous-wal-archiving). For the [archive-push](#archive-push) and [archive-get](#archive-get) commands, you must specify the [remote options](#remote-mode-options) that point to backup host with backup catalog.

- Run [backup](#backup) or [restore](#restore) commands with [remote options](#remote-mode-options) **on backup host**. pg_probackup connects to the remote system via SSH and creates a backup locally or restores the previously taken backup on the remote system, respectively.

For example, to create archive full backup using remote mode through SSH connection to user `postgres` on host with address `192.168.0.2` via port `2302`, run:

    pg_probackup backup -B backup_dir --instance instance_name -b FULL --remote-user=postgres --remote-host=192.168.0.2 --remote-port=2302

For example, to restore latest backup on remote system using remote mode through SSH connection to user `postgres` on host with address `192.168.0.2` via port `2302`, run:

    pg_probackup restore -B backup_dir --instance instance_name --remote-user=postgres --remote-host=192.168.0.2 --remote-port=2302

>NOTE: The remote backup mode is currently unavailable for Windows systems.

### Running pg_probackup on Parallel Threads

[Backup](#backup), [restore](#restore), [merge](#merge), [delete](#delete), [checkdb](#checkdb) and [validate](#validate) processes can be executed on several parallel threads. This can significantly speed up pg_probackup operation given enough resources (CPU cores, disk and network bandwidth).

Parallel execution is controlled by the `-j/--threads` command line option. For example, to create a backup using four parallel threads, run:

    pg_probackup backup -B backup_dir --instance instance_name -b FULL -j 4

>NOTE: Parallel restore applies only to copying data from the backup catalog to the data directory of the cluster. When PostgreSQL server is started, WAL records need to be replayed, and this cannot be done in parallel.

### Configuring pg_probackup

Once the backup catalog is initialized and a new backup instance is added, you can use the pg_probackup.conf configuration file located in the '*backup_dir*/backups/*instance_name*' directory to fine-tune pg_probackup configuration.

For example, [backup](#backup) and [checkdb](#checkdb) commands uses a regular PostgreSQL connection. To avoid specifying these options each time on the command line, you can set them in the pg_probackup.conf configuration file using the [set-config](#set-config) command.

>NOTE: It is **not recommended** to edit pg_probackup.conf manually.

Initially, pg_probackup.conf contains the following settings:

- PGDATA — the path to the data directory of the cluster to back up.
- system-identifier — the unique identifier of the PostgreSQL instance.

Additionally, you can define [remote](#remote-mode-options), [retention](#retention-options), [logging](#logging-options) and [compression](#compression-options) settings using the `set-config` command:

    pg_probackup set-config -B backup_dir --instance instance_name
    [--external-dirs=external_directory_path] [remote_options] [connection_options] [retention_options] [logging_options]

To view the current settings, run the following command:

    pg_probackup show-config -B backup_dir --instance instance_name

You can override the settings defined in pg_probackup.conf when running pg_probackups [commands](#commands) via corresponding environment variables and/or command line options.

### Specifying Connection Settings

If you define connection settings in the 'pg_probackup.conf' configuration file, you can omit connection options in all the subsequent pg_probackup commands. However, if the corresponding environment variables are set, they get higher priority. The options provided on the command line overwrite both environment variables and configuration file settings.

If nothing is given, the default values are taken. By default pg_probackup tries to use local connection via Unix domain socket (localhost on Windows) and tries to get the database name and the user name from the PGUSER environment variable or the current OS user name.

### Managing the Backup Catalog

With pg_probackup, you can manage backups from the command line:

- View available backups
- View available WAL Archive Information
- Validate backups
- Merge backups
- Delete backups
- Viewing Backup Information

To view the list of existing backups for every instance, run the command:

    pg_probackup show -B backup_dir

pg_probackup displays the list of all the available backups. For example:

```
BACKUP INSTANCE 'node'
============================================================================================================================================
 Instance    Version  ID      Recovery time           Mode    WAL      Current/Parent TLI    Time    Data   Start LSN    Stop LSN    Status 
============================================================================================================================================
 node        10       P7XDQV  2018-04-29 05:32:59+03  DELTA   STREAM     1 / 1                11s    19MB   0/15000060   0/15000198  OK
 node        10       P7XDJA  2018-04-29 05:28:36+03  PTRACK  STREAM     1 / 1                21s    32MB   0/13000028   0/13000198  OK
 node        10       P7XDHU  2018-04-29 05:27:59+03  PAGE    STREAM     1 / 1                31s    33MB   0/11000028   0/110001D0  OK
 node        10       P7XDHB  2018-04-29 05:27:15+03  FULL    STREAM     1 / 0                11s    39MB   0/F000028    0/F000198   OK
```

For each backup, the following information is provided:

- Instance — the instance name.
- Version — PostgreSQL major version.
- ID — the backup identifier.
- Recovery time — the earliest moment for which you can restore the state of the database cluster.
- Mode — the method used to take this backup. Possible values: FULL, PAGE, DELTA, PTRACK.
- WAL — the WAL delivery mode. Possible values: STREAM and ARCHIVE.
- Current/Parent TLI — timeline identifiers of current backup and its parent.
- Time — the time it took to perform the backup.
- Data — the size of the data files in this backup. This value does not include the size of WAL files.
- Start LSN — WAL log sequence number corresponding to the start of the backup process.
- Stop LSN — WAL log sequence number corresponding to the end of the backup process.
- Status — backup status. Possible values:

    - OK — the backup is complete and valid.
    - DONE — the backup is complete, but was not validated.
    - RUNNING — the backup is in progress.
    - MERGING — the backup is being merged.
    - DELETING — the backup files are being deleted.
    - CORRUPT — some of the backup files are corrupted.
    - ERROR — the backup was aborted because of an unexpected error.
    - ORPHAN — the backup is invalid because one of its parent backups is corrupt or missing.

You can restore the cluster from the backup only if the backup status is OK or DONE.

To get more detailed information about the backup, run the show with the backup ID:

    pg_probackup show -B backup_dir --instance instance_name -i backup_id

The sample output is as follows:

```
#Configuration
backup-mode = FULL
stream = false
compress-alg = zlib
compress-level = 1
from-replica = false

#Compatibility
block-size = 8192
wal-block-size = 8192
checksum-version = 1
program-version = 2.1.3
server-version = 10

#Result backup info
timelineid = 1
start-lsn = 0/04000028
stop-lsn = 0/040000f8
start-time = '2017-05-16 12:57:29'
end-time = '2017-05-16 12:57:31'
recovery-xid = 597
recovery-time = '2017-05-16 12:57:31'
data-bytes = 22288792
wal-bytes = 16777216
status = OK
parent-backup-id = 'PT8XFX'
primary_conninfo = 'user=backup passfile=/var/lib/pgsql/.pgpass port=5432 sslmode=disable sslcompression=1 target_session_attrs=any'
```

To get more detailed information about the backup in json format, run the show with the backup ID:

    pg_probackup show -B backup_dir --instance instance_name --format=json -i backup_id

The sample output is as follows:

```
[
    {
        "instance": "node",
        "backups": [
            {
                "id": "PT91HZ",
                "parent-backup-id": "PT8XFX",
                "backup-mode": "DELTA",
                "wal": "ARCHIVE",
                "compress-alg": "zlib",
                "compress-level": 1,
                "from-replica": false,
                "block-size": 8192,
                "xlog-block-size": 8192,
                "checksum-version": 1,
                "program-version": "2.1.3",
                "server-version": "10",
                "current-tli": 16,
                "parent-tli": 2,
                "start-lsn": "0/8000028",
                "stop-lsn": "0/8000160",
                "start-time": "2019-06-17 18:25:11+03",
                "end-time": "2019-06-17 18:25:16+03",
                "recovery-xid": 0,
                "recovery-time": "2019-06-17 18:25:15+03",
                "data-bytes": 106733,
                "wal-bytes": 16777216,
                "primary_conninfo": "user=backup passfile=/var/lib/pgsql/.pgpass port=5432 sslmode=disable sslcompression=1 target_session_attrs=any",
                "status": "OK"
            }
        ]
    }
]
```

#### Viewing WAL Archive Information

To view the information about WAL archive for every instance, run the command:

    pg_probackup show -B backup_dir [--instance instance_name] --archive

pg_probackup displays the list of all the available WAL files grouped by timelines. For example:

```
ARCHIVE INSTANCE 'node'
===================================================================================================================
 TLI  Parent TLI  Switchpoint  Min Segno         Max Segno         N segments  Size    Zratio  N backups  Status
===================================================================================================================
 5    1           0/B000000    000000000000000B  000000000000000C  2           685kB   48.00   0          OK
 4    3           0/18000000   0000000000000018  000000000000001A  3           648kB   77.00   0          OK
 3    2           0/15000000   0000000000000015  0000000000000017  3           648kB   77.00   0          OK
 2    1           0/B000108    000000000000000B  0000000000000015  5           892kB   94.00   1          DEGRADED
 1    0           0/0          0000000000000001  000000000000000A  10          8774kB  19.00   1          OK

```

For each backup, the following information is provided:

- TLI — timeline identifier.
- Parent TLI — identifier of timeline TLI branched off.
- Switchpoint — LSN of the moment when the timeline branched off from "Parent TLI".
- Min Segno — number of the first existing WAL segment belonging to the timeline.
- Max Segno — number of the last existing WAL segment belonging to the timeline.
- N segments — number of WAL segments belonging to the timeline.
- Size — the size files take on disk.
- Zratio - compression ratio calculated as "N segments" * wal_seg_size / "Size".
- N backups — number of backups belonging to the timeline. To get the details about backups, use json format.
- Status — archive status for this exact timeline. Possible values:
	- OK — all WAL segments between Min and Max are present.
	- DEGRADED — some WAL segments between Min and Max are lost. To get details about lost files, use json format.

To get more detailed information about the WAL archive in json format, run the command:

    pg_probackup show -B backup_dir [--instance instance_name] --archive --format=json

The sample output is as follows:

```
[
    {
        "instance": "replica",
        "timelines": [
            {
                "tli": 5,
                "parent-tli": 1,
                "switchpoint": "0/B000000",
                "min-segno": "000000000000000B",
                "max-segno": "000000000000000C",
                "n-segments": 2,
                "size": 685320,
                "zratio": 48.00,
                "closest-backup-id": "PXS92O",
                "status": "OK",
                "lost-segments": [],
                "backups": []
            },
            {
                "tli": 4,
                "parent-tli": 3,
                "switchpoint": "0/18000000",
                "min-segno": "0000000000000018",
                "max-segno": "000000000000001A",
                "n-segments": 3,
                "size": 648625,
                "zratio": 77.00,
                "closest-backup-id": "PXS9CE",
                "status": "OK",
                "lost-segments": [],
                "backups": []
            },
            {
                "tli": 3,
                "parent-tli": 2,
                "switchpoint": "0/15000000",
                "min-segno": "0000000000000015",
                "max-segno": "0000000000000017",
                "n-segments": 3,
                "size": 648911,
                "zratio": 77.00,
                "closest-backup-id": "PXS9CE",
                "status": "OK",
                "lost-segments": [],
                "backups": []
            },
            {
                "tli": 2,
                "parent-tli": 1,
                "switchpoint": "0/B000108",
                "min-segno": "000000000000000B",
                "max-segno": "0000000000000015",
                "n-segments": 5,
                "size": 892173,
                "zratio": 94.00,
                "closest-backup-id": "PXS92O",
                "status": "DEGRADED",
                "lost-segments": [
                    {
                        "begin-segno": "000000000000000D",
                        "end-segno": "000000000000000E"
                    },
                    {
                        "begin-segno": "0000000000000010",
                        "end-segno": "0000000000000012"
                    }
                ],
                "backups": [
                    {
                        "id": "PXS9CE",
                        "backup-mode": "FULL",
                        "wal": "ARCHIVE",
                        "compress-alg": "none",
                        "compress-level": 1,
                        "from-replica": "false",
                        "block-size": 8192,
                        "xlog-block-size": 8192,
                        "checksum-version": 1,
                        "program-version": "2.1.5",
                        "server-version": "10",
                        "current-tli": 2,
                        "parent-tli": 0,
                        "start-lsn": "0/C000028",
                        "stop-lsn": "0/C000160",
                        "start-time": "2019-09-13 21:43:26+03",
                        "end-time": "2019-09-13 21:43:30+03",
                        "recovery-xid": 0,
                        "recovery-time": "2019-09-13 21:43:29+03",
                        "data-bytes": 104674852,
                        "wal-bytes": 16777216,
                        "primary_conninfo": "user=backup passfile=/var/lib/pgsql/.pgpass port=5432 sslmode=disable sslcompression=1 target_session_attrs=any",
                        "status": "OK"
                    }
                ]
            },
            {
                "tli": 1,
                "parent-tli": 0,
                "switchpoint": "0/0",
                "min-segno": "0000000000000001",
                "max-segno": "000000000000000A",
                "n-segments": 10,
                "size": 8774805,
                "zratio": 19.00,
                "closest-backup-id": "",
                "status": "OK",
                "lost-segments": [],
                "backups": [
                    {
                        "id": "PXS92O",
                        "backup-mode": "FULL",
                        "wal": "ARCHIVE",
                        "compress-alg": "none",
                        "compress-level": 1,
                        "from-replica": "true",
                        "block-size": 8192,
                        "xlog-block-size": 8192,
                        "checksum-version": 1,
                        "program-version": "2.1.5",
                        "server-version": "10",
                        "current-tli": 1,
                        "parent-tli": 0,
                        "start-lsn": "0/4000028",
                        "stop-lsn": "0/6000028",
                        "start-time": "2019-09-13 21:37:36+03",
                        "end-time": "2019-09-13 21:38:45+03",
                        "recovery-xid": 0,
                        "recovery-time": "2019-09-13 21:37:30+03",
                        "data-bytes": 25987319,
                        "wal-bytes": 50331648,
                        "primary_conninfo": "user=backup passfile=/var/lib/pgsql/.pgpass port=5432 sslmode=disable sslcompression=1 target_session_attrs=any",
                        "status": "OK"
                    }
                ]
            }
        ]
    },
    {
        "instance": "master",
        "timelines": [
            {
                "tli": 1,
                "parent-tli": 0,
                "switchpoint": "0/0",
                "min-segno": "0000000000000001",
                "max-segno": "000000000000000B",
                "n-segments": 11,
                "size": 8860892,
                "zratio": 20.00,
                "status": "OK",
                "lost-segments": [],
                "backups": [
                    {
                        "id": "PXS92H",
                        "parent-backup-id": "PXS92C",
                        "backup-mode": "PAGE",
                        "wal": "ARCHIVE",
                        "compress-alg": "none",
                        "compress-level": 1,
                        "from-replica": "false",
                        "block-size": 8192,
                        "xlog-block-size": 8192,
                        "checksum-version": 1,
                        "program-version": "2.1.5",
                        "server-version": "10",
                        "current-tli": 1,
                        "parent-tli": 1,
                        "start-lsn": "0/4000028",
                        "stop-lsn": "0/50000B8",
                        "start-time": "2019-09-13 21:37:29+03",
                        "end-time": "2019-09-13 21:37:31+03",
                        "recovery-xid": 0,
                        "recovery-time": "2019-09-13 21:37:30+03",
                        "data-bytes": 1328461,
                        "wal-bytes": 33554432,
                        "primary_conninfo": "user=backup passfile=/var/lib/pgsql/.pgpass port=5432 sslmode=disable sslcompression=1 target_session_attrs=any",
                        "status": "OK"
                    },
                    {
                        "id": "PXS92C",
                        "backup-mode": "FULL",
                        "wal": "ARCHIVE",
                        "compress-alg": "none",
                        "compress-level": 1,
                        "from-replica": "false",
                        "block-size": 8192,
                        "xlog-block-size": 8192,
                        "checksum-version": 1,
                        "program-version": "2.1.5",
                        "server-version": "10",
                        "current-tli": 1,
                        "parent-tli": 0,
                        "start-lsn": "0/2000028",
                        "stop-lsn": "0/2000160",
                        "start-time": "2019-09-13 21:37:24+03",
                        "end-time": "2019-09-13 21:37:29+03",
                        "recovery-xid": 0,
                        "recovery-time": "2019-09-13 21:37:28+03",
                        "data-bytes": 24871902,
                        "wal-bytes": 16777216,
                        "primary_conninfo": "user=backup passfile=/var/lib/pgsql/.pgpass port=5432 sslmode=disable sslcompression=1 target_session_attrs=any",
                        "status": "OK"
                    }
                ]
            }
        ]
    }
]
```

Most fields are consistent with plain format, with some exceptions:

- size is in bytes.
- 'closest-backup-id' attribute contain ID of valid backup closest to the timeline, located on some of the previous timelines. This backup is the closest starting point to reach the timeline from other timelines by PITR. Closest backup always has a valid status, either OK or DONE. If such backup do not exists, then string is empty.
- DEGRADED timelines contain 'lost-segments' array with information about intervals of missing segments. In OK timelines 'lost-segments' array is empty.
- 'N backups' attribute is replaced with 'backups' array containing backups belonging to the timeline. If timeline has no backups, then 'backups' array is empty.

### Configuring Backup Retention Policy

By default, all backup copies created with pg_probackup are stored in the specified backup catalog. To save disk space, you can configure retention policy and periodically clean up redundant backup copies accordingly.

To configure retention policy, set one or more of the following variables in the pg_probackup.conf file via [set-config](#set-config):

    --retention-redundancy=redundancy
Specifies **the number of full backup copies** to keep in the backup catalog.

    --retention-window=window
Defines the earliest point in time for which pg_probackup can complete the recovery. This option is set in **the number of days** from the current moment. For example, if `retention-window=7`, pg_probackup must delete all backup copies that are older than seven days, with all the corresponding WAL files.

If both `--retention-redundancy` and `--retention-window` options are set, pg_probackup keeps backup copies that satisfy at least one condition. For example, if you set `--retention-redundancy=2` and `--retention-window=7`, pg_probackup purges the backup catalog to keep only two full backup copies and all backups that are newer than seven days.

To clean up the backup catalog in accordance with retention policy, run:

    pg_probackup delete -B backup_dir --instance instance_name --delete-expired

pg_probackup deletes all backup copies that do not conform to the defined retention policy.

If you would like to also remove the WAL files that are no longer required for any of the backups, add the `--delete-wal` flag:

    pg_probackup delete -B backup_dir --instance instance_name --delete-expired --delete-wal

>NOTE: Alternatively, you can use the `--delete-expired`, `--merge-expired`, `--delete-wal` flags and the `--retention-window` and `--retention-redundancy` options together with the [backup](#backup) command to remove and merge the outdated backup copies once the new backup is created.

Since incremental backups require that their parent full backup and all the preceding incremental backups are available, if any of such backups expire, they still cannot be removed while at least one incremental backup in this chain satisfies the retention policy. To avoid keeping expired backups that are still required to restore an active incremental one, you can merge them with this backup using the `--merge-expired` flag when running [backup](#backup) or [delete](#delete) commands.

Suppose you have backed up the *node* instance in the *backup_dir* directory, with the `--retention-window` option is set to *7*, and you have the following backups available on April 10, 2019:

```
BACKUP INSTANCE 'node'
===========================================================================================================================================
 Instance    Version  ID      Recovery time           Mode    WAL      Current/Parent TLI   Time   Data    Start LSN    Stop LSN    Status 
===========================================================================================================================================
 node        10       P7XDHR  2019-04-10 05:27:15+03  FULL    STREAM     1 / 0               11s   200MB   0/18000059   0/18000197  OK
 node        10       P7XDQV  2019-04-08 05:32:59+03  PAGE    STREAM     1 / 0               11s    19MB   0/15000060   0/15000198  OK
 node        10       P7XDJA  2019-04-03 05:28:36+03  DELTA   STREAM     1 / 0               21s    32MB   0/13000028   0/13000198  OK
 ---------------------------------------retention window-------------------------------------------------------------
 node        10       P7XDHU  2019-04-02 05:27:59+03  PAGE    STREAM     1 / 0               31s    33MB   0/11000028   0/110001D0  OK
 node        10       P7XDHB  2019-04-01 05:27:15+03  FULL    STREAM     1 / 0               11s   200MB   0/F000028    0/F000198   OK
 node        10       P7XDFT  2019-03-29 05:26:25+03  FULL    STREAM     1 / 0               11s   200MB   0/D000028    0/D000198   OK
```

Even though P7XDHB and P7XDHU backups are outside the retention window, they cannot be removed as it invalidates the succeeding incremental backups P7XDJA and P7XDQV that are still required, so, if you run the [delete](#delete) command with the `--delete-expired` flag, only the P7XDFT full backup will be removed.

With the `--merge-expired` option, the P7XDJA backup is merged with the underlying P7XDHU and P7XDHB backups and becomes a full one, so there is no need to keep these expired backups anymore:

    pg_probackup delete -B backup_dir --instance node --delete-expired --merge-expired
    pg_probackup show -B backup_dir

```
BACKUP INSTANCE 'node'
============================================================================================================================================
 Instance    Version  ID      Recovery time           Mode    WAL      Current/Parent TLI    Time   Data    Start LSN    Stop LSN    Status
============================================================================================================================================
 node        10       P7XDHR  2019-04-10 05:27:15+03  FULL    STREAM     1 / 0                11s   200MB   0/18000059   0/18000197  OK
 node        10       P7XDQV  2019-04-08 05:32:59+03  DELTA   STREAM     1 / 0                11s    19MB   0/15000060   0/15000198  OK
 node        10       P7XDJA  2019-04-04 05:28:36+03  FULL    STREAM     1 / 0                 5s   200MB   0/13000028   0/13000198  OK
```

>NOTE: The Time field for the merged backup displays the time required for the merge.

### Merging Backups

As you take more and more incremental backups, the total size of the backup catalog can substantially grow. To save disk space, you can merge incremental backups to their parent full backup by running the merge command, specifying the backup ID of the most recent incremental backup you would like to merge:

    pg_probackup merge -B backup_dir --instance instance_name -i backup_id

This command merges the specified incremental backup to its parent full backup, together with all incremental backups between them. Once the merge is complete, the incremental backups are removed as redundant. Thus, the merge operation is virtually equivalent to retaking a full backup and removing all the outdated backups, but it allows to save much time, especially for large data volumes, I/O and network traffic in case of [remote](#using-pg_probackup-in-the-remote-mode) backup.

Before the merge, pg_probackup validates all the affected backups to ensure that they are valid. You can check the current backup status by running the [show](#show) command with the backup ID:

    pg_probackup show -B backup_dir --instance instance_name -i backup_id

If the merge is still in progress, the backup status is displayed as MERGING. The merge is idempotent, so you can restart the merge if it was interrupted.

### Deleting Backups

To delete a backup that is no longer required, run the following command:

    pg_probackup delete -B backup_dir --instance instance_name -i backup_id

This command will delete the backup with the specified *backup_id*, together with all the incremental backups that descend from *backup_id* if any. This way you can delete some recent incremental backups, retaining the underlying full backup and some of the incremental backups that follow it.

To delete obsolete WAL files that are not necessary to restore any of the remaining backups, use the `--delete-wal` flag:

    pg_probackup delete -B backup_dir --instance instance_name --delete-wal

To delete backups that are expired according to the current retention policy, use the `--delete-expired` flag:

    pg_probackup delete -B backup_dir --instance instance_name --delete-expired

Note that expired backups cannot be removed while at least one incremental backup that satisfies the retention policy is based on them. If you would like to minimize the number of backups still required to keep incremental backups valid, specify the `--merge-expired` flag when running this command:

    pg_probackup delete -B backup_dir --instance instance_name --delete-expired --merge-expired

In this case, pg_probackup searches for the oldest incremental backup that satisfies the retention policy and merges this backup with the underlying full and incremental backups that have already expired, thus making it a full backup. Once the merge is complete, the remaining expired backups are deleted.

Before merging or deleting backups, you can run the `delete` command with the `--dry-run` flag, which displays the status of all the available backups according to the current retention policy, without performing any irreversible actions.

## Command-Line Reference
### Commands

This section describes pg_probackup commands. Some commands require mandatory parameters and can take additional options. Optional parameters encased in square brackets. For detailed descriptions of options, see the section [Options](#options).

#### version

    pg_probackup version

Prints pg_probackup version.

#### help

    pg_probackup help [command]

Displays the synopsis of pg_probackup commands. If one of the pg_probackup commands is specified, shows detailed information about the options that can be used with this command.

#### init

    pg_probackup init -B backup_dir [--help]

Initializes the backup catalog in *backup_dir* that will store backup copies, WAL archive and meta information for the backed up database clusters. If the specified *backup_dir* already exists, it must be empty. Otherwise, pg_probackup displays a corresponding error message.

For details, see the secion [Initializing the Backup Catalog](#initializing-the-backup-catalog).

#### add-instance

    pg_probackup add-instance -B backup_dir -D data_dir --instance instance_name
    [--help]

Initializes a new backup instance inside the backup catalog *backup_dir* and generates the pg_probackup.conf configuration file that controls pg_probackup settings for the cluster with the specified *data_dir* data directory.

For details, see the section [Adding a New Backup Instance](#adding-a-new-backup-instance).

#### del-instance

    pg_probackup del-instance -B backup_dir --instance instance_name
    [--help]

Deletes all backups and WAL files associated with the specified instance.

#### set-config

    pg_probackup set-config -B backup_dir --instance instance_name
    [--help] [--pgdata=pgdata-path]
    [--retention-redundancy=redundancy][--retention-window=window]
    [--compress-algorithm=compression_algorithm] [--compress-level=compression_level]
    [-d dbname] [-h host] [-p port] [-U username]
    [--archive-timeout=timeout] [--external-dirs=external_directory_path]
    [--restore-command=cmdline]
    [remote_options] [remote_archive_options] [logging_options]

Adds the specified connection, compression, retention, logging and external directory settings into the pg_probackup.conf configuration file, or modifies the previously defined values.

For all available settings, see the [Options](#options) section.

It is **not recommended** to edit pg_probackup.conf manually.

#### show-config

    pg_probackup show-config -B backup_dir --instance instance_name [--format=plain|json]

Displays the contents of the pg_probackup.conf configuration file located in the '*backup_dir*/backups/*instance_name*' directory. You can specify the `--format=json` option to return the result in the JSON format. By default, configuration settings are shown as plain text.

To edit pg_probackup.conf, use the [set-config](#set-config) command.

#### show

    pg_probackup show -B backup_dir
    [--help] [--instance instance_name [-i backup_id | --archive]] [--format=plain|json]

Shows the contents of the backup catalog. If *instance_name* and *backup_id* are specified, shows detailed information about this backup. You can specify the `--format=json` option to return the result in the JSON format. If `--archive` option is specified, shows the content of WAL archive of the backup catalog.

By default, the contents of the backup catalog is shown as plain text.

For details on usage, see the sections [Managing the Backup Catalog](#managing-the-backup-catalog) and [Viewing WAL Archive Information](#viewing-wal-archive-information).


#### backup

    pg_probackup backup -B backup_dir -b backup_mode --instance instance_name
    [--help] [-j num_threads] [--progress]
    [-C] [--stream [-S slot_name] [--temp-slot]] [--backup-pg-log]
    [--no-validate] [--skip-block-validation]
    [-w --no-password] [-W --password]
    [--archive-timeout=timeout] [--external-dirs=external_directory_path]
    [connection_options] [compression_options] [remote_options]
    [retention_options] [logging_options]

Creates a backup copy of the PostgreSQL instance. The *backup_mode* option specifies the backup mode to use.

    -b mode
    --backup-mode=mode

Specifies the backup mode to use. Possible values are:

- FULL — creates a full backup that contains all the data files of the cluster to be restored.
- DELTA — reads all data files in the data directory and creates an incremental backup for pages that have changed since the previous backup.
- PAGE — creates an incremental PAGE backup based on the WAL files that have changed since the previous full or incremental backup was taken.
- PTRACK — creates an incremental PTRACK backup tracking page changes on the fly.

```
-C
--smooth-checkpoint
```
Spreads out the checkpoint over a period of time. By default, pg_probackup tries to complete the checkpoint as soon as possible.

    --stream
Makes an [STREAM](#stream-mode) backup that includes all the necessary WAL files by streaming them from the database server via replication protocol.

    --temp-slot
Creates a temporary physical replication slot for streaming WAL from the backed up PostgreSQL instance. It ensures that all the required WAL segments remain available if WAL is rotated while the backup is in progress. This flag can only be used together with the `--stream` flag. Default slot name is `pg_probackup_slot`, which can be changed via option `--slot/-S`.

    -S slot_name
    --slot=slot_name
Specifies the replication slot for WAL streaming. This option can only be used together with the `--stream` flag.

    --backup-pg-log
Includes the log directory into the backup. This directory usually contains log messages. By default, log directory is excluded.

    -E external_directory_path
    --external-dirs=external_directory_path
Includes the specified directory into the backup. This option is useful to back up scripts, sql dumps and configuration files located outside of the data directory. If you would like to back up several external directories, separate their paths by a colon on Unix and a semicolon on Windows.

    --archive-timeout=wait_time
Sets in seconds the timeout for WAL segment archiving and streaming. By default pg_probackup waits 300 seconds.

    --skip-block-validation
Disables block-level checksum verification to speed up backup.

    --no-validate
Skips automatic validation after successfull backup. You can use this flag if you validate backups regularly and would like to save time when running backup operations.

Additionally [Connection Options](#connection-options), [Retention Options](#retention-options), [Remote Mode Options](#remote-mode-options), [Compression Options](#compression-options), [Logging Options](#logging-options) and [Common Options](#common-options) can be used.

For details on usage, see the section [Creating a Backup](#creating-a-backup).

#### restore

    pg_probackup restore -B backup_dir --instance instance_name
    [--help] [-D data_dir] [-i backup_id]
    [-j num_threads] [--progress]
    [-T OLDDIR=NEWDIR] [--external-mapping=OLDDIR=NEWDIR] [--skip-external-dirs]
    [-R | --restore-as-replica] [--no-validate] [--skip-block-validation]
    [--restore-command=cmdline]
    [recovery_options] [logging_options] [remote_options]
    [partial_restore_options] [remote_archive_options]

Restores the PostgreSQL instance from a backup copy located in the *backup_dir* backup catalog. If you specify a [recovery target option](#recovery-target-options), pg_probackup will find the closest backup and restores it to the specified recovery target. Otherwise, the most recent backup is used.

    -R | --restore-as-replica
Writes a minimal recovery.conf in the output directory to facilitate setting up a standby server. The password is not included. If the replication connection requires a password, you must specify the password manually.

    -T OLDDIR=NEWDIR
    --tablespace-mapping=OLDDIR=NEWDIR

Relocates the tablespace from the OLDDIR to the NEWDIR directory at the time of recovery. Both OLDDIR and NEWDIR must be absolute paths. If the path contains the equals sign (=), escape it with a backslash. This option can be specified multiple times for multiple tablespaces.

    --external-mapping=OLDDIR=NEWDIR
Relocates an external directory included into the backup from the OLDDIR to the NEWDIR directory at the time of recovery. Both OLDDIR and NEWDIR must be absolute paths. If the path contains the equals sign (=), escape it with a backslash. This option can be specified multiple times for multiple directories.

    --skip-external-dirs
Skip external directories included into the backup with the `--external-dirs` option. The contents of these directories will not be restored.

    --skip-block-validation
Disables block-level checksum verification to speed up validation. During automatic validation before restore only file-level checksums will be verified.

    --no-validate
Skips backup validation. You can use this flag if you validate backups regularly and would like to save time when running restore operations.

    --restore-command=cmdline
Set the [restore_command](https://www.postgresql.org/docs/current/archive-recovery-settings.html#RESTORE-COMMAND) parameter to specified command. Example: `--restore-command='cp /mnt/server/archivedir/%f "%p"'`

Additionally [Recovery Target Options](#recovery-target-options), [Remote Mode Options](#remote-mode-options), [Remote WAL Archive Options](#remote-wal-archive-options), [Logging Options](#logging-options), [Partial Restore](#partial-restore) and [Common Options](#common-options) can be used.

For details on usage, see the section [Restoring a Cluster](#restoring-a-cluster).

#### checkdb

    pg_probackup checkdb
    [-B backup_dir] [--instance instance_name] [-D data_dir]
    [--help] [-j num_threads] [--progress]
    [--skip-block-validation] [--amcheck] [--heapallindexed]
    [connection_options] [logging_options]

Verifies the PostgreSQL database cluster correctness by detecting physical and logical corruption.

    --amcheck
Performs logical verification of indexes for the specified PostgreSQL instance if no corruption was found while checking data files. You must have the `amcheck` extention or the `amcheck_next` extension installed in the database to check its indexes. For databases without amcheck, index verification will be skipped.

    --skip-block-validation
Skip validation of data files. Can be used only with `--amcheck` flag, so only logical verification of indexes is performed.

    --heapallindexed
Checks that all heap tuples that should be indexed are actually indexed. You can use this flag only together with the `--amcheck` flag. Can be used only with `amcheck` extension of version 2.0 and `amcheck_next` extension of any version.

Additionally [Connection Options](#connection-options) and [Logging Options](#logging-options) can be used.

For details on usage, see the section [Verifying a Cluster](#verifying-a-cluster).

#### validate

    pg_probackup validate -B backup_dir
    [--help] [--instance instance_name] [-i backup_id]
    [-j num_threads] [--progress]
    [--skip-block-validation]
    [recovery_target_options] [logging_options]

Verifies that all the files required to restore the cluster are present and not corrupted. If *instance_name* is not specified, pg_probackup validates all backups available in the backup catalog. If you specify the *instance_name* without any additional options, pg_probackup validates all the backups available for this backup instance. If you specify the *instance_name* with a [recovery target options](#recovery-target-options) and/or a *backup_id*, pg_probackup checks whether it is possible to restore the cluster using these options.

For details, see the section [Validating a Backup](#validating-a-backup).

#### merge

    pg_probackup merge -B backup_dir --instance instance_name -i backup_id
    [--help] [-j num_threads] [--progress]
    [logging_options]

Merges the specified incremental backup to its parent full backup, together with all incremental backups between them, if any. As a result, the full backup takes in all the merged data, and the incremental backups are removed as redundant.

For details, see the section [Merging Backups](#merging-backups).

#### delete

    pg_probackup delete -B backup_dir --instance instance_name
    [--help] [-j num_threads] [--progress]
    [--delete-wal] {-i backup_id | --delete-expired [--merge-expired] | --merge-expired}
    [--dry-run]
    [logging_options]

Deletes backup with specified *backip_id* or launches the retention purge of backups and archived WAL that do not satisfy the current retention policies.

For details, see the sections [Deleting Backups](#deleting-backups), [Retention Options](#retention-options) and [Configuring Backup Retention Policy](#configuring-backup-retention-policy).

#### archive-push

    pg_probackup archive-push -B backup_dir --instance instance_name
    --wal-file-path=wal_file_path --wal-file-name=wal_file_name
    [--help] [--compress] [--compress-algorithm=compression_algorithm]
    [--compress-level=compression_level] [--overwrite]
    [remote_options] [logging_options]

Copies WAL files into the corresponding subdirectory of the backup catalog and validates the backup instance by *instance_name* and *system-identifier*. If parameters of the backup instance and the cluster do not match, this command fails with the following error message: “Refuse to push WAL segment segment_name into archive. Instance parameters mismatch.” For each WAL file moved to the backup catalog, you will see the following message in PostgreSQL logfile: “pg_probackup archive-push completed successfully”.

If the files to be copied already exist in the backup catalog, pg_probackup computes and compares their checksums. If the checksums match, archive-push skips the corresponding file and returns successful execution code. Otherwise, archive-push fails with an error. If you would like to replace WAL files in the case of checksum mismatch, run the archive-push command with the `--overwrite` flag.

Copying is done to temporary file with `.part` suffix or, if [compression](#compression-options) is used, with `.gz.part` suffix. After copy is done, atomic rename is performed. This algorihtm ensures that failed archive-push will not stall continuous archiving and that concurrent archiving from multiple sources into single WAL archive has no risk of archive corruption.
Copied to archive WAL segments are synced to disk.

You can use `archive-push` in [archive_command](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-ARCHIVE-COMMAND) PostgreSQL parameter to set up [continous WAl archiving](#setting-up-continuous-wal-archiving).

For details, see sections [Archiving Options](#archiving-options) and [Compression Options](#compression-options).

#### archive-get

    pg_probackup archive-get -B backup_dir --instance instance_name --wal-file-path=wal_file_path --wal-file-name=wal_file_name
    [--help] [remote_options] [logging_options]

Copies WAL files from the corresponding subdirectory of the backup catalog to the cluster's write-ahead log location. This command is automatically set by pg_probackup as part of the `restore_command` in 'recovery.conf' when restoring backups using a WAL archive. You do not need to set it manually.

### Options

This section describes command-line options for pg_probackup commands. If the option value can be derived from an environment variable, this variable is specified below the command-line option, in the uppercase. Some values can be taken from the pg_probackup.conf configuration file located in the backup catalog.

For details, see the section [Configuring pg_probackup](#configuring-pg_probackup).

If an option is specified using more than one method, command-line input has the highest priority, while the pg_probackup.conf settings have the lowest priority.

#### Common Options
The list of general options.

    -B directory
    --backup-path=directory
    BACKUP_PATH
Specifies the absolute path to the backup catalog. Backup catalog is a directory where all backup files and meta information are stored. Since this option is required for most of the pg_probackup commands, you are recommended to specify it once in the BACKUP_PATH environment variable. In this case, you do not need to use this option each time on the command line.

    -D directory
    --pgdata=directory
    PGDATA
Specifies the absolute path to the data directory of the database cluster. This option is mandatory only for the [add-instance](#add-instance) command. Other commands can take its value from the PGDATA environment variable, or from the pg_probackup.conf configuration file.

    -i backup_id
    -backup-id=backup_id
Specifies the unique identifier of the backup.

    -j num_threads
    --threads=num_threads
Sets the number of parallel threads for backup, restore, merge, validation and verification processes.

    --progress
Shows the progress of operations.

    --help
Shows detailed information about the options that can be used with this command.

#### Recovery Target Options

If [continuous WAL archiving](#setting-up-continuous-wal-archiving) is configured, you can use one of these options together with [restore](#restore) or [validate](#validate) commands to specify the moment up to which the database cluster must be restored or validated.

    --recovery-target=immediate|latest
Defines when to stop the recovery:

- `immediate` value stops the recovery after reaching the consistent state of the specified backup, or the latest available backup if the `-i/--backup_id` option is omitted.
- `latest` value continues the recovery until all WAL segments available in the archive are applied.

Default value of `--recovery-target` depends on WAL delivery method of restored backup, `immediate` for STREAM backup and `latest` for ARCHIVE.

    --recovery-target-timeline=timeline
Specifies a particular timeline to which recovery will proceed. By default, the timeline of the specified backup is used.

    --recovery-target-lsn=lsn
Specifies the LSN of the write-ahead log location up to which recovery will proceed. Can be used only when restoring database cluster of major version 10 or higher.

    --recovery-target-name=recovery_target_name
Specifies a named savepoint up to which to restore the cluster data.

    --recovery-target-time=time
Specifies the timestamp up to which recovery will proceed.

    --recovery-target-xid=xid
Specifies the transaction ID up to which recovery will proceed.

    --recovery-target-inclusive=boolean
Specifies whether to stop just after the specified recovery target (true), or just before the recovery target (false). This option can only be used together with `--recovery-target-name`, `--recovery-target-time`, `--recovery-target-lsn` or `--recovery-target-xid` options. The default depends on [recovery_target_inclusive](https://www.postgresql.org/docs/current/recovery-target-settings.html#RECOVERY-TARGET-INCLUSIVE) parameter.

    --recovery-target-action=pause|promote|shutdown
    Default: pause 
Specifies [the action](https://www.postgresql.org/docs/current/recovery-target-settings.html#RECOVERY-TARGET-ACTION) the server should take when the recovery target is reached.

#### Retention Options

You can use these options together with [backup](#backup) and [delete](#delete) commands.

For details on configuring retention policy, see the sections [Configuring Backup Retention Policy](#configuring-backup-retention-policy).

    --retention-redundancy=redundancy
    Default: 0 
Specifies the number of full backup copies to keep in the data directory. Must be a positive integer. The zero value disables this setting.

    --retention-window=window
    Default: 0 
Number of days of recoverability. Must be a positive integer. The zero value disables this setting.

    --delete-wal
Deletes WAL files that are no longer required to restore the cluster from any of the existing backups.

    --delete-expired
Deletes backups that do not conform to the retention policy defined in the pg_probackup.conf configuration file.

    --merge-expired
Merges the oldest incremental backup that satisfies the requirements of retention policy with its parent backups that have already expired.

    --dry-run
Displays the current status of all the available backups, without deleting or merging expired backups, if any.

#### Logging Options

You can use these options with any command.

    --log-level-console=log_level
    Default: info 
Controls which message levels are sent to the console log. Valid values are `verbose`, `log`, `info`, `warning`, `error` and `off`. Each level includes all the levels that follow it. The later the level, the fewer messages are sent. The `off` level disables console logging.

>NOTE: all console log messages are going to stderr, so output from [show](#show) and [show-config](#show-config) commands do not mingle with log messages.

    --log-level-file=log_level
    Default: off 
Controls which message levels are sent to a log file. Valid values are `verbose`, `log`, `info`, `warning`, `error` and `off`. Each level includes all the levels that follow it. The later the level, the fewer messages are sent. The `off` level disables file logging.

    --log-filename=log_filename
    Default: pg_probackup.log
Defines the filenames of the created log files. The filenames are treated as a strftime pattern, so you can use %-escapes to specify time-varying filenames.

For example, if you specify the 'pg_probackup-%u.log' pattern, pg_probackup generates a separate log file for each day of the week, with %u replaced by the corresponding decimal number: pg_probackup-1.log for Monday, pg_probackup-2.log for Tuesday, and so on.

This option takes effect if file logging is enabled by the `log-level-file` option.

    --error-log-filename=error_log_filename
    Default: none
Defines the filenames of log files for error messages only. The filenames are treated as a strftime pattern, so you can use %-escapes to specify time-varying filenames.

For example, if you specify the 'error-pg_probackup-%u.log' pattern, pg_probackup generates a separate log file for each day of the week, with %u replaced by the corresponding decimal number: error-pg_probackup-1.log for Monday, error-pg_probackup-2.log for Tuesday, and so on.

This option is useful for troubleshooting and monitoring.

    --log-directory=log_directory
    Default: $BACKUP_PATH/log/
Defines the directory in which log files will be created. You must specify the absolute path. This directory is created lazily, when the first log message is written.

    --log-rotation-size=log_rotation_size
    Default: 0
Maximum size of an individual log file. If this value is reached, the log file is rotated once a pg_probackup command is launched, except help and version commands. The zero value disables size-based rotation. Supported units: kB, MB, GB, TB (kB by default).

    --log-rotation-age=log_rotation_age
    Default: 0
Maximum lifetime of an individual log file. If this value is reached, the log file is rotated once a pg_probackup command is launched, except help and version commands. The time of the last log file creation is stored in $BACKUP_PATH/log/log_rotation. The zero value disables time-based rotation. Supported units: ms, s, min, h, d (min by default).

#### Connection Options

You can use these options together with [backup](#backup) and [checkdb](#checkdb) commands.

All [libpq environment variables](https://www.postgresql.org/docs/current/libpq-envars.html) are supported.

    -d dbname
    --dbname=dbname
    PGDATABASE
Specifies the name of the database to connect to. The connection is used only for managing backup process, so you can connect to any existing database. If this option is not provided on the command line, PGDATABASE environment variable, or the pg_probackup.conf configuration file, pg_probackup tries to take this value from the PGUSER environment variable, or from the current user name if PGUSER variable is not set.

    -h host
    --host=host
    PGHOST
    Default: local socket 
Specifies the host name of the system on which the server is running. If the value begins with a slash, it is used as a directory for the Unix domain socket.

    -p port
    --port=port
    PGPORT
    Default: 5432 
Specifies the TCP port or the local Unix domain socket file extension on which the server is listening for connections.

    -U username
    --username=username
    PGUSER
User name to connect as.

    -w
    --no-password
 Disables a password prompt. If the server requires password authentication and a password is not available by other means such as a [.pgpass](https://www.postgresql.org/docs/current/libpq-pgpass.html) file or PGPASSWORD environment variable, the connection attempt will fail. This flag can be useful in batch jobs and scripts where no user is present to enter a password.

    -W
    --password
Forces a password prompt.

#### Compression Options

You can use these options together with [backup](#backup) and [archive-push](#archive-push) commands.

    --compress-algorithm=compression_algorithm
    Default: none
Defines the algorithm to use for compressing data files. Possible values are `zlib`, `pglz`, and `none`. If set to zlib or pglz, this option enables compression. By default, compression is disabled.
For the [archive-push](#archive-push) command, the pglz compression algorithm is not supported.

    --compress-level=compression_level
    Default: 1
Defines compression level (0 through 9, 0 being no compression and 9 being best compression). This option can be used together with `--compress-algorithm` option.

    --compress
Alias for `--compress-algorithm=zlib` and `--compress-level=1`.

#### Archiving Options

These options can be used with [archive-push](#archive-push) command in [archive_command](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-ARCHIVE-COMMAND) setting and [archive-get](#archive-get) command in [restore_command](https://www.postgresql.org/docs/current/archive-recovery-settings.html#RESTORE-COMMAND) setting.

Additionally [Remote Mode Options](#remote-mode-options) and [Logging Options](#logging-options) can be used.

    --wal-file-path=wal_file_path
Provides the path to the WAL file in `archive_command` and `restore_command`. The `%p` variable as value for this option is required for correct processing.

    --wal-file-name=wal_file_name
Provides the name of the WAL file in `archive_command` and `restore_command`. The `%f` variable as value is required for correct processing.

    --overwrite
Overwrites archived WAL file. Use this flag together with the [archive-push](#archive-push) command if the specified subdirectory of the backup catalog already contains this WAL file and it needs to be replaced with its newer copy. Otherwise, archive-push reports that a WAL segment already exists, and aborts the operation. If the file to replace has not changed, archive-push skips this file regardless of the `--overwrite` flag.

#### Remote Mode Options

This section describes the options related to running pg_probackup operations remotely via SSH. These options can be used with [add-instance](#add-instance), [set-config](#set-config), [backup](#backup), [restore](#restore), [archive-push](#archive-push) and [archive-get](#archive-get) commands.

For details on configuring and usage of remote operation mode, see the sections [Configuring the Remote Mode](#configuring-the-remote-mode) and [Using pg_probackup in the Remote Mode](#using-pg_probackup-in-the-remote-mode).

    --remote-proto=proto
Specifies the protocol to use for remote operations. Currently only the SSH protocol is supported. Possible values are:

- `ssh` enables the remote backup mode via SSH. This is the Default value.
- `none` explicitly disables the remote mode.

You can omit this option if the `--remote-host` option is specified.

    --remote-host=destination
Specifies the remote host IP address or hostname to connect to.

    --remote-port=port
    Default: 22 
Specifies the remote host port to connect to.

    --remote-user=username
    Default: current user
Specifies remote host user for SSH connection. If you omit this option, the current user initiating the SSH connection is used.

    --remote-path=path
Specifies pg_probackup installation directory on the remote system.

    --ssh-options=ssh_options
Specifies a string of SSH command-line options. For example, the following options can used to set keep-alive for ssh connections opened by pg_probackup: `--ssh-options='-o ServerAliveCountMax=5 -o ServerAliveInterval=60'`. Full list of possible options can be found on [ssh_config manual page](https://man.openbsd.org/ssh_config.5).

#### Remote WAL Archive Options

This section describes the options used to provide the values for [Remote Mode Options](#remote-mode-options) for [archive-get](#archive-get) command when restoring ARCHIVE backup or performing PITR.

    --archive-host=destination
Provides the value for `--remote-host` option of `archive-get` command.

    --archive-port=port
    Default: 22
Provides the value for `--remote-port` option of `archive-get` command.

    --archive-user=username
    Default: PostgreSQL user
Provides the value for `--remote-user` option of `archive-get` command. If you omit this option, the the user running PostgreSQL cluster is used.

#### Partial Restore Options

This section describes the options related to partial restore of a cluster from backup. These options can be used with [restore](#restore) command.

    --db-exclude=dbname
Specifies database name to exclude from restore. All other databases in the cluster will be restored as usual, including `template0` and `template1`. This option can be specified multiple times for multiple databases.

    --db-include=dbname
Specifies database name to restore from backup. All other databases in the cluster will not be restored, with exception of `template0` and `template1`. This option can be specified multiple times for multiple databases.

#### Replica Options

This section describes the options related to taking a backup from standby.

>NOTE: Starting from pg_probackup 2.0.24, backups can be taken from standby without connecting to the master server, so these options are no longer required. In lower versions, pg_probackup had to connect to the master to determine recovery time — the earliest moment for which you can restore a consistent state of the database cluster.

    --master-db=dbname
     Default: postgres, the default PostgreSQL database.
Deprecated. Specifies the name of the database on the master server to connect to. The connection is used only for managing the backup process, so you can connect to any existing database. Can be set in the pg_probackup.conf using the [set-config](#set-config) command.

    --master-host=host
Deprecated. Specifies the host name of the system on which the master server is running.

    --master-port=port
    Default: 5432, the PostgreSQL default port.
Deprecated. Specifies the TCP port or the local Unix domain socket file extension on which the master server is listening for connections.

    --master-user=username
    Default: postgres, the PostgreSQL default user name.
Deprecated. User name to connect as.

    --replica-timeout=timeout
    Default: 300 sec
Deprecated. Wait time for WAL segment streaming via replication, in seconds. By default, pg_probackup waits 300 seconds. You can also define this parameter in the pg_probackup.conf configuration file using the [set-config](#set-config) command.

## Authors
Postgres Professional, Moscow, Russia.

## Credits
pg_probackup utility is based on pg_arman, that was originally written by NTT and then developed and maintained by Michael Paquier.
