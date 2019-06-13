# pg_probackup
---
**pg_probackup** is a utility to manage backup and recovery of Postgres Pro database clusters. It is designed to perform periodic backups of the Postgres Pro instance that enable you to restore the server in case of a failure. pg_probackup supports Postgres Pro 9.5 or higher.

### Synopsis

`pg_probackup init -B backupdir`

`pg_probackup add-instance -B backupdir -D datadir --instance instance_name`

`pg_probackup del-instance -B backupdir --instance instance_name`

`pg_probackup set-config -B backupdir --instance instance_name [option...]`

`pg_probackup show-config -B backupdir --instance instance_name [--format=format]`

`pg_probackup backup -B backupdir --instance instance_name -b backup_mode [option...]`

`pg_probackup merge -B backupdir --instance instance_name -i backup_id [option...]`

`pg_probackup restore -B backupdir --instance instance_name [option...]`

`pg_probackup validate -B backupdir [option...]`

`pg_probackup show -B backupdir [option...]`

`pg_probackup delete -B backupdir --instance instance_name { -i backup_id | --wal | --expired }`

`pg_probackup archive-push -B backupdir --instance instance_name --wal-file-path %p --wal-file-name %f [option...]`

`pg_probackup archive-get -B backupdir --instance instance_name --wal-file-path %p --wal-file-name %f`

`pg_probackup checkdb -B backupdir --instance instance_name -D datadir [option...]`

`pg_probackup version`

`pg_probackup help [command]`

* [Installation and Setup](#installation-and-setup)
* [Command-Line Reference](#command-line-reference)
* [Usage](#usage)

### Overview

As compared to other backup solutions, pg_probackup offers the following benefits that can help you implement different backup strategies and deal with large amounts of data:

- Choosing between full and page-level incremental backups to speed up backup and recovery
- Implementing a single backup strategy for multi-server Postgres Pro clusters
- Automatic data consistency checks and on-demand backup validation without actual data recovery
- Managing backups in accordance with retention policy
- Running backup, restore, and validation processes on multiple parallel threads
- Storing backup data in a compressed state to save disk space
- Taking backups from a standby server to avoid extra load on the master server
- Extended logging settings
- Custom commands to simplify WAL log archiving
- Backing up files and directories located outside of Postgres Pro data directory, such as configuration or log files 

To manage backup data, pg_probackup creates a *backup catalog*. This directory stores all backup files with additional meta information, as well as WAL archives required for point-in-time recovery. You can store backups for different instances in separate subdirectories of a single backup catalog.

Using pg_probackup, you can take full or incremental backups:
- Full backups contain all the data files required to restore the database cluster from scratch.
- Incremental backups only store the data that has changed since the previous backup. It allows to decrease the backup size and speed up backup operations. pg_probackup supports the following modes of incremental backups:
    - PAGE backup. In this mode, pg_probackup scans all WAL files in the archive from the moment the previous full or incremental backup was taken. Newly created backups contain only the pages that were mentioned in WAL records. This requires all the WAL files since the previous backup to be present in the WAL archive. If the size of these files is comparable to the total size of the database cluster files, speedup is smaller, but the backup still takes less space.
    - DELTA backup. In this mode, pg_probackup reads all data files in the data directory and copies only those pages that has changed since the previous backup. Continuous archiving is not necessary for this mode to operate. Note that this mode can impose read-only I/O pressure equal to a full backup.
    - PTRACK backup. In this mode, Postgres Pro tracks page changes on the fly. Continuous archiving is not necessary for it to operate. Each time a relation page is updated, this page is marked in a special PTRACK bitmap for this relation. As one page requires just one bit in the PTRACK fork, such bitmaps are quite small. Tracking implies some minor overhead on the database server operation, but speeds up incremental backups significantly. 

Regardless of the chosen backup type, all backups taken with pg_probackup support the following archiving strategies:
- Autonomous backups include all the files required to restore the cluster to a consistent state at the time the backup was taken. Even if continuous archiving is not set up, the required WAL segments are included into the backup.
- Archive backups rely on continuous archiving. Such backups enable cluster recovery to an arbitrary point after the backup was taken (point-in-time recovery). 

### Limitations

pg_probackup currently has the following limitations:
- Creating backups from a remote server is currently not supported on Windows systems.
- The server from which the backup was taken and the restored server must be compatible by the block_size and wal_block_size parameters and have the same major release number. 

### Installation and Setup

The pg_probackup package is provided as part of the Postgres Pro distribution. Once you have pg_probackup installed, complete the following setup:

- Initialize the backup catalog.
- Add a new backup instance to the backup catalog.
- Configure the database cluster to enable pg_probackup backups. 

##### Initializing the Backup Catalog
pg_probackup stores all WAL and backup files in the corresponding subdirectories of the backup catalog.

To initialize the backup catalog, run the following command:

`pg_probackup init -B backupdir`

where backupdir is the backup catalog. If the backupdir already exists, it must be empty. Otherwise, pg_probackup returns an error.

pg_probackup creates the backupdir backup catalog, with the following subdirectories:
- *wal/* — directory for WAL files.
- *backups/* — directory for backup files. 

Once the backup catalog is initialized, you can add a new backup instance.

##### Adding a New Backup Instance
pg_probackup can store backups for multiple database clusters in a single backup catalog. To set up the required subdirectories, you must add a backup instance to the backup catalog for each database cluster you are going to back up.

To add a new backup instance, run the following command:

`pg_probackup add-instance -B backupdir -D datadir --instance instance_name [--external-dirs=external_directory_path] [remote_backup_options]`

where:
- *datadir* is the data directory of the cluster you are going to back up. To set up and use pg_probackup, write access to this directory is required.
-  *instance_name* is the name of the subdirectories that will store WAL and backup files for this cluster.
- The optional *--external-dirs* parameter provides the path to one or more directories to include into the backup that are located outside of the data directory. To specify several external directories, separate their paths by a colon. 

pg_probackup creates the instance_name subdirectories under the *backups/* and *wal/* directories of the backup catalog. The *backups/instance_name* directory contains the *pg_probackup.conf* configuration file that controls backup and restore settings for this backup instance. If you run this command with the optional *--external-dirs* parameter, this setting is added to *pg_probackup.conf*, so the specified external directories will be backed up each time you create a backup of this instance. For details on how to fine-tune pg_probackup configuration, see the section called “Configuring pg_probackup”.

The backup catalog must belong to the file system of the database server. The user launching pg_probackup must have full access to the contents of the backup catalog. If you specify the path to the backup catalog in the *BACKUP_PATH* environment variable, you can omit the corresponding option when running pg_probackup commands.

Since pg_probackup uses a regular PostgreSQL connection and the replication protocol, pg_probackup commands require connection options. To avoid specifying these options each time on the command line, you can set them in the pg_probackup.conf configuration file using the *set-config* command. For details, see the section called “Configuring pg_probackup”.

##### Configuring the Database Cluster

Although pg_probackup can be used by a superuser, it is recommended to create a separate user or role with the minimum permissions required for the chosen backup strategy. In these configuration instructions, the *backup* role is used as an example.

To enable backups, the following rights are required:

```CREATE ROLE backup WITH LOGIN;
GRANT USAGE ON SCHEMA pg_catalog TO backup;
GRANT EXECUTE ON FUNCTION current_setting(text) TO backup;
GRANT EXECUTE ON FUNCTION pg_is_in_recovery() TO backup;
GRANT EXECUTE ON FUNCTION pg_start_backup(text, boolean, boolean) TO backup;
GRANT EXECUTE ON FUNCTION pg_stop_backup() TO backup;
GRANT EXECUTE ON FUNCTION pg_stop_backup(boolean, boolean) TO backup;
GRANT EXECUTE ON FUNCTION pg_create_restore_point(text) TO backup;
GRANT EXECUTE ON FUNCTION pg_switch_wal() TO backup;
GRANT EXECUTE ON FUNCTION txid_current() TO backup;
GRANT EXECUTE ON FUNCTION txid_current_snapshot() TO backup;
GRANT EXECUTE ON FUNCTION txid_snapshot_xmax(txid_snapshot) TO backup;
```

Since pg_probackup needs to read cluster files directly, pg_probackup must be started on behalf of an OS user that has read access to all files and directories inside the data directory (PGDATA) you are going to back up.

Depending on whether you are going to use autonomous or archive backup strategies, Postgres Pro cluster configuration will differ, as specified in the sections below. To back up the database cluster from a standby server or create PTRACK backups, additional setup is required. For details, see the section called “PTRACK Backup” and the section called “Backup from Standby”.

##### Setting up Autonomous Backups

To set up the cluster for autonomous backups, complete the following steps:
- Grant the REPLICATION privilege to the backup role:
    `ALTER ROLE backup WITH REPLICATION;`
- In the pg_hba.conf file, allow replication on behalf of the backup role.
- Modify the postgresql.conf configuration file of the Postgres Pro server, as follows:
    - Make sure the max_wal_senders parameter is set high enough to leave at least one session available for the backup process.
    - Set the *wal_level* parameter to be *replica* or higher. 

If you are going to take PAGE backups, you also have to configure WAL archiving as explained in the section called “Setting up Archive Backups”. 

Once these steps are complete, you can start taking FULL, PAGE, or DELTA backups from the master server. If you are going to take backups from standby or use PTRACK backups, you must also complete additional setup, as explained in the section called “Backup from Standby” and the section called “PTRACK Backup”, respectively.

##### Setting up Archive Backups
To set up the cluster for archive backups, complete the following steps:
- Configure the following parameters in postgresql.conf to enable continuous archiving on the Postgres Pro server:
    - Make sure the wal_level parameter is set to replica or higher.
    - Set the archive_mode parameter. If you are configuring backups on master, archive_mode must be set to on. To perform archiving on standby, set this parameter to always.
    - Set the archive_command variable, as follows:
        ```archive_command = 'pg_probackup archive-push -B backupdir --instance instance_name --wal-file-path %p --wal-file-name %f'```
        where backupdir and instance_name refer to the already initialized backup catalog instance for this database cluster. 

Once these steps are complete, you can start taking FULL, PAGE, or DELTA backups from the master server. If you are going to take backups from standby or use PTRACK backups, you must also complete additional setup, as explained in the section called “Backup from Standby” and the section called “PTRACK Backup”, respectively.

##### Backup from Standby

For Postgres Pro 9.6 or higher, pg_probackup can take backups from a standby server. This requires the following additional setup:

- On the standby server, allow replication connections:
    - Set the max_wal_senders and hot_standby parameters in postgresql.conf.
    - Configure host-based authentication in pg_hba.conf. 
- On the master server, enable full_page_writes in postgresql.conf. 



>NOTE: Archive backup from the standby server has the following limitations:
- If the standby is promoted to the master during archive backup, the backup fails.
- All WAL records required for the backup must contain sufficient full-page writes. This requires you to enable full_page_writes on the master, and not to use a tool like pg_compresslog as archive_command to remove full-page writes from WAL files. 

##### PTRACK Backup

If you are going to use PTRACK backups, complete the following additional steps:
- In postgresql.conf, set ptrack_enable to on.
- Grant the rights to execute ptrack functions to the backup role: 
    ```
    GRANT EXECUTE ON FUNCTION pg_ptrack_clear() TO backup;
    GRANT EXECUTE ON FUNCTION pg_ptrack_get_and_clear(oid, oid) TO backup;
    ```
    The backup role must have access to all the databases of the cluster. 

### Command-Line Reference
##### Commands

This section describes pg_probackup commands. Some commands require mandatory options and can take additional options. For detailed descriptions, see the section called “Options”.

**init**

    pg_probackup init -B backupdir
Initializes the backupdir backup catalog that will store backup copies, WAL archive, and meta information for the backed up database clusters. If the specified backupdir already exists, it must be empty. Otherwise, pg_probackup displays a corresponding error message. 

**add-instance**

    pg_probackup add-instance -B backupdir -D datadir --instance instance_name [--external-dirs=external_directory_path]
Initializes a new backup instance inside the backup catalog backupdir and generates the pg_probackup.conf configuration file that controls backup and restore settings for the cluster with the specified datadir data directory. For details, see the section called “Adding a New Backup Instance”. 

**del-instance**

    pg_probackup del-instance -B backupdir --instance instance_name
Deletes all backup and WAL files associated with the specified instance. 

**set-config**

    pg_probackup set-config -B backupdir --instance instance_name
    [--log-level-console=log_level] [--log-level-file=log_level] [--log-filename=log_filename]
    [--error-log-filename=error_log_filename] [--log-directory=log_directory]
    [--log-rotation-size=log_rotation_size] [--log-rotation-age=log_rotation_age]
    [--retention-redundancy=redundancy][--retention-window=window]
    [--compress-algorithm=compression_algorithm] [--compress-level=compression_level]
    [-d dbname] [-h host] [-p port] [-U username]
    [--archive-timeout=timeout] [--external-dirs=external_directory_path]
    [remote_backup_options]
Adds the specified connection, retention, logging or replica, and compression, and external directory settings into the pg_probackup.conf configuration file, or modifies the previously defined values. 

**show-config**
	pg_probackup show-config -B backupdir --instance instance_name [--format=plain|json]
Displays the contents of the pg_probackup.conf configuration file located in the backupdir/backups/instance_name directory. You can specify the --format=json option to return the result in the JSON format. By default, configuration settings are shown as plain text.
To edit pg_probackup.conf, use the set-config command.
It is not allowed to edit pg_probackup.conf directly. 

**backup**

    pg_probackup backup -B backupdir -b backup_mode --instance instance_name
    [-C] [--stream [-S slot_name] [--temp-slot]] [--backup-pg-log] [--external-dirs=external_directory_path]
    [--delete-expired] [--merge-expired] [--delete-wal] [--no-validate] [--skip-block-validation]
    [--retention-redundancy=redundancy] [--retention-window=window]
    [-d dbname] [-h host] [-p port] [-U username]
    [-w --no-password] [-W --password]
    [--archive-timeout=timeout]
    [--compress] [--compress-algorithm=compression_algorithm] [--compress-level=compression_level]
    [-j num_threads][--progress]
    [logging_options]
    [remote_backup_options]
Creates a backup copy of the Postgres Pro instance. The backup_mode option specifies the backup mode to use. For details, see the section called “Creating a Backup”. 

**merge**

    pg_probackup merge -B backupdir --instance instance_name -i backup_id
    [-j num_threads][--progress]
    [logging_options]
    [remote_backup_options]

Merges the specified incremental backup to its parent full backup, together with all incremental backups between them, if any. As a result, the full backup takes in all the merged data, and the incremental backups are removed as redundant. For details, see the section called “Merging Backups”. 

**restore**

    pg_probackup restore -B backupdir --instance instance_name
    [-D datadir]
    [-i backup_id] [{--recovery-target=immediate|latest | --recovery-target-time=time | --recovery-target-xid=xid | --recovery-target-lsn=lsn | --recovery-target-name=recovery_target_name} [--recovery-target-inclusive=boolean]]
    [--recovery-target-timeline=timeline] [-T OLDDIR=NEWDIR]
    [--external-mapping=OLDDIR=NEWDIR] [--skip-external-dirs]
    [--recovery-target-action=pause|promote|shutdown]
    [-R | --restore-as-replica] [--no-validate] [--skip-block-validation]
    [-j num_threads] [--progress]
    [logging_options]
    [remote_backup_options]

Restores the Postgres Pro instance from a backup copy located in the backupdir backup catalog. If you specify a recovery target option, pg_probackup restores the database cluster up to the corresponding recovery target. Otherwise, the most recent backup is used. 

**validate**

    pg_probackup validate -B backupdir
    [--instance instance_name]
    [-i backup_id] [{--recovery-target-time=time | --recovery-target-xid=xid | --recovery-target-lsn=lsn | --recovery-target-name=recovery_target_name } [--recovery-target-inclusive=boolean]]
    [--recovery-target-timeline=timeline] [--skip-block-validation]
    [-j num_threads] [--progress]

Verifies that all the files required to restore the cluster are present and not corrupted. If you specify the instance_name without any additional options, pg_probackup validates all the backups available for this backup instance. If you specify the instance_name with a recovery target option or a backup_id, pg_probackup checks whether it is possible to restore the cluster using these options. If instance_name is not specified, pg_probackup validates all backups available in the backup catalog. 

**show**

    pg_probackup show -B backupdir
    [--instance instance_name [-i backup_id]] [--format=plain|json]

Shows the contents of the backup catalog. If instance_name and backup_id are specified, shows detailed information about this backup. You can specify the --format=json option to return the result in the JSON format. By default, the contents of the backup catalog is shown as plain text. 

**delete**

    pg_probackup delete -B backupdir --instance instance_name
     [--wal] {-i backup_id | --expired [--merge-expired] | --merge-expired} [--dry-run]

Deletes backup or WAL files of the specified backup instance from the backupdir backup catalog:
- The wal option removes the WAL files that are no longer required to restore the cluster from any of the existing backups.
- The -i option removes the specified backup copy.
- The expired option removes the backups that are expired according to the current retention policy. If used together with merge-expired, this option takes effect only after the merge is performed.

- The merge-expired option merges the oldest incremental backup that satisfies the requirements of retention policy with its parent backups that have already expired.

- The dry-run option displays the current status of all the available backups, without deleting or merging expired backups, if any. 

**archive-push**

    pg_probackup archive-push -B backupdir --instance instance_name
    --wal-file-path %p --wal-file-name %f'
    [--compress][--compress-algorithm=compression_algorithm][--compress-level=compression_level] [--overwrite]
    [remote_backup_options]

Copies WAL files into the corresponding subdirectory of the backup catalog and validates the backup instance by instance_name, system-identifier, and PGDATA. If parameters of the backup instance and the cluster do not match, this command fails with the following error message: “Refuse to push WAL segment segment_name into archive. Instance parameters mismatch.” For each WAL file moved to the backup catalog, you will see the following message in Postgres Pro logfile: “pg_probackup archive-push completed successfully”. If the files to be copied already exist in the backup catalog, pg_probackup computes and compares their checksums. If the checksums match, archive-push skips the corresponding file and returns successful execution code. Otherwise, archive-push fails with an error. If you would like to replace WAL files in the case of checksum mismatch, run the archive-push command with the --overwrite option.

You can set archive-push as archive_command in postgresql.conf to perform archive backups. 

**archive-get**

    pg_probackup archive-get -B backupdir --instance instance_name
    --wal-file-path %p --wal-file-name %f'
    [remote backup options]

Moves WAL files from the corresponding subdirectory of the backup catalog to the cluster's write-ahead log location. This command is automatically set by pg_probackup as restore_command in recovery.conf when restoring backups using a WAL archive. You do not need to set it manually. 

**checkdb**

    pg_probackup checkdb -D datadir [-B backupdir] [--instance instance_name]
    [--amcheck [--heapallindexed] [--skip-block-validation]]
    [--progress] [-j num_threads]

Validates all data files located in the specified data directory by performing block-level checksum verification and page header sanity checks. If run with the --amcheck option, this command also performs logical verification of all indexes in the specified Postgres Pro instance using the amcheck extension. 

**version**

    pg_probackup version

Prints pg_probackup version. 

**help**

    pg_probackup help [command]

Displays the synopsis of pg_probackup commands. If one of the pg_probackup commands is specified, shows detailed information about the options that can be used with this command.

##### Options
This section describes all command-line options for pg_probackup commands. If the option value can be derived from an environment variable, this variable is specified below the command-line option, in the uppercase. Some values can be taken from the pg_probackup.conf configuration file located in the backup catalog. For details, see the section called “Configuring pg_probackup”.

If an option is specified using more than one method, command-line input has the highest priority, while the pg_probackup.conf settings have the lowest priority.

**Common Options**
```
    -B directory
    --backup-path=directory
    BACKUP_PATH
```
Specifies the absolute path to the backup catalog. Backup catalog is a directory where all backup files and meta information are stored. Since this option is required for most of the pg_probackup commands, you are recommended to specify it once in the BACKUP_PATH environment variable. In this case, you do not need to use this option each time on the command line. 

    -D directory
    --pgdata=directory
    PGDATA
Specifies the absolute path to the data directory of the database cluster. This option is mandatory only for the init command. Other commands can take its value from the PGDATA environment variable, or from the pg_probackup.conf configuration file.

    -i backup_id
    -backup-id=backup_id
Specifies the unique identifier of the backup. 

    --skip-block-validation

Disables block-level checksum verification to speed up validation. If this option is used with backup, restore, and validate commands, only file-level checksums will be verified. When used with the checkdb command run with the --amcheck option, --skip-block-validation completely disables validation of data files. 

    -j num_threads
    --threads=num_threads
Sets the number of parallel threads for backup, recovery, and backup validation processes. 

    --progress
Shows the progress of operations. 

**Backup Options**

The following options can be used together with the backup command.

    -b mode
    --backup-mode=mode

Specifies the backup mode to use. Possible values are:
- FULL — creates a full backup that contains all the data files of the cluster to be restored.
- DELTA — reads all data files in the data directory and creates an incremental backup for pages that have changed since the previous backup.
- PAGE — creates an incremental PAGE backup based on the WAL files that have changed since the previous full or incremental backup was taken.
- PTRACK — creates an incremental PTRACK backup tracking page changes on the fly. 

For details, see the section called “Creating a Backup”.

    -C
    --smooth-checkpoint
    SMOOTH_CHECKPOINT
Spreads out the checkpoint over a period of time. By default, pg_probackup tries to complete the checkpoint as soon as possible. 

    --stream
Makes an autonomous backup that includes all the necessary WAL files by streaming them from the database server via replication protocol. 

    -S slot_name
    --slot=slot_name
Specifies the replication slot for WAL streaming. This option can only be used together with the --stream option. 

    --temp-slot
Creates a temporary physical replication slot for streaming WAL from the backed up Postgres Pro instance. It ensures that all the required WAL segments remain available if WAL is rotated while the backup is in progress. This option can only be used together with the --stream option. 

    --backup-pg-log
Includes the log directory into the backup. This directory usually contains log messages. By default, log directory is excluded. 

    -E external_directory_path
    --external-dirs=external_directory_path

Includes the specified directory into the backup. This option is useful to back up configuration files located outside of the data directory. If you would like to back up several external directories, separate their paths by a colon. 

    --archive-timeout=wait_time
Sets the timeout for WAL segment archiving, in seconds. By default, pg_probackup waits 300 seconds. 

    --delete-expired
After a backup copy is successfully created, deletes backups that are expired according to the current retention policy. You can also clean up the expired backups by running the delete command with the expired option. If used together with merge-expired, this option takes effect after the merge is performed. For details, see the section called “Configuring Backup Retention Policy”. 

    --merge-expired
After a backup copy is successfully created, merges the oldest incremental backup that satisfies the requirements of retention policy with its parent backups that have already expired. 

    --delete-wal
After a backup copy is successfully created, removes redundant WAL files in accordance with the current retention policy. You can also clean up the expired WAL files by running the delete command with the wal option. For details, see the section called “Configuring Backup Retention Policy”. 

**Restore Options**

    --recovery-target-action=pause|promote|shutdown
    Default: pause 
Specifies the action the server should take when the recovery target is reached, similar to the recovery_target_action option in the recovery.conf configuration file.


    -R | --restore-as-replica
Writes a minimal recovery.conf in the output directory to facilitate setting up a standby server. The password is not included. If the replication connection requires a password, you must specify the password manually. 

    -T OLDDIR=NEWDIR
    --tablespace-mapping=OLDDIR=NEWDIR

Relocates the tablespace from the OLDDIR to the NEWDIR directory at the time of recovery. Both OLDDIR and NEWDIR must be absolute paths. If the path contains the equals sign (=), escape it with a backslash. This option can be specified multiple times for multiple tablespaces. 

    --external-mapping=OLDDIR=NEWDIR
Relocates an external directory included into the backup from the OLDDIR to the NEWDIR directory at the time of recovery. Both OLDDIR and NEWDIR must be absolute paths. If the path contains the equals sign (=), escape it with a backslash. This option can be specified multiple times for multiple directories. 

    --skip-external-dirs
Skip external directories included into the backup with the --external-dirs option. The contents of these directories will not be restored. 

    --recovery-target-timeline=timeline
Specifies a particular timeline to restore the cluster into. By default, the timeline of the specified backup is used. 

    --no-validate
Skips backup validation. You can use this option if you validate backups regularly and would like to save time when running backup or restore operations. 

**Recovery Target Options**
If continuous WAL archiving is configured, you can use one of these options together with restore or validate commands to specify the moment up to which the database cluster must be restored.

    --recovery-target=immediate|latest
Defines when to stop the recovery:
- The immediate value stops the recovery after reaching the consistent state of the specified backup, or the latest available backup if the -i option is omitted.
- The latest value continues the recovery until all WAL segments available in the archive are applied. 


    --recovery-target-lsn=lsn
Specifies the LSN of the write-ahead log location up to which recovery will proceed. 

    --recovery-target-name=recovery_target_name
Specifies a named savepoint up to which to restore the cluster data. 

    --recovery-target-time=time
Specifies the timestamp up to which recovery will proceed. 

    --recovery-target-xid=xid
Specifies the transaction ID up to which recovery will proceed. 

    --recovery-target-inclusive=boolean
Specifies whether to stop just after the specified recovery target (true), or just before the recovery target (false). This option can only be used together with recovery-target-name, recovery-target-time, recovery-target-lsn, or recovery-target-xid options. The default value is taken from the recovery_target_inclusive variable. 

**Delete Options**

    --wal
Deletes WAL files that are no longer required to restore the cluster from any of the existing backups. 

    --expired
Deletes backups that do not conform to the retention policy defined in the pg_probackup.conf configuration file. For details, see the section called “Configuring Backup Retention Policy”. 

    --merge-expired
Merges the oldest incremental backup that satisfies the requirements of retention policy with its parent backups that have already expired. 

    --dry-run
Displays the current status of all the available backups, without deleting or merging expired backups, if any. 

**Retention Options**
For details on configuring retention policy, see the section called “Configuring Backup Retention Policy”.

    --retention-redundancy=redundancy
    Default: 0 
Specifies the number of full backup copies to keep in the data directory. Must be a positive integer. The zero value disables this setting.

    --retention-window=window
    Default: 0 
Number of days of recoverability. The zero value disables this setting.

**Logging Options**

    --log-level-console=log_level
    Default: info 
Controls which message levels are sent to the console log. Valid values are verbose, log, info, notice, warning, error, and off. Each level includes all the levels that follow it. The later the level, the fewer messages are sent. The off level disables console logging.

    --log-level-file=log_level
    Default: off 
Controls which message levels are sent to a log file. Valid values are verbose, log, info, notice, warning, error, and off. Each level includes all the levels that follow it. The later the level, the fewer messages are sent. The off level disables file logging.

    --log-filename=log_filename
    Default: pg_probackup.log 
Defines the filenames of the created log files. The filenames are treated as a strftime pattern, so you can use %-escapes to specify time-varying filenames, as explained in log_filename. For example, if you specify the pg_probackup-%u.log pattern, pg_probackup generates a separate log file for each day of the week, with %u replaced by the corresponding decimal number: pg_probackup-1.log for Monday, pg_probackup-2.log for Tuesday, and so on.
This option takes effect if file logging is enabled by the log-level-file option.

    --error-log-filename=error_log_filename
    Default: none 
Defines the filenames of log files for error messages. The filenames are treated as a strftime pattern, so you can use %-escapes to specify time-varying file names, as explained in log_filename.
If error-log-filename is not set, pg_probackup writes all error messages to stderr.

    --log-directory=log_directory
    Default: $BACKUP_PATH/log/ 
Defines the directory in which log files will be created. You must specify the absolute path. This directory is created lazily, when the first log message is written.

    --log-rotation-size=log_rotation_size
    Default: 0
Maximum size of an individual log file. If this value is reached, the log file is rotated once a pg_probackup command is launched, except help and version commands. The zero value disables size-based rotation. Supported units: kB, MB, GB, TB (kB by default).

    --log-rotation-age=log_rotation_age
    Default: 0 
Maximum lifetime of an individual log file. If this value is reached, the log file is rotated once a pg_probackup command is launched, except help and version commands. The time of the last log file creation is stored in $BACKUP_PATH/log/log_rotation. The zero value disables time-based rotation. Supported units: ms, s, min, h, d (min by default).

**Connection Options**

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
 Disables a password prompt. If the server requires password authentication and a password is not available by other means such as a .pgpass file, the connection attempt will fail. This option can be useful in batch jobs and scripts where no user is present to enter a password. 

    -W
    --password
Forces a password prompt. 

**Compression Options**

    --compress
Enables compression for data files. You can specify the compression algorithm and level using the --compress-algorithm and --compress-level options, respectively. If you omit these options, --compress uses zlib compression algorithm with compression level 1.
By default, compression is disabled. 

    --compress-algorithm=compression_algorithm
Defines the algorithm to use for compressing data files. Possible values are zlib, pglz, and none. If set to zlib or pglz, this option enables compression, regardless of whether the --compress option is specified. By default, compression is disabled.
For the archive-push command, the pglz compression algorithm is not supported. 

    --compress-level=compression_level
    Default: 1 
Defines compression level (0 through 9, 0 being no compression and 9 being best compression). This option can only be used together with --compress or --compress-algorithm options.

**Replica Options**
This section describes the options related to taking a backup from standby.
>NOTE: Starting from pg_probackup 2.0.24, backups can be taken from standby without connecting to the master server, so these options are no longer required. In lower versions, pg_probackup had to connect to the master to determine recovery time — the earliest moment for which you can restore a consistent state of the database cluster.

    --master-db=dbname
     Default: postgres, the default Postgres Pro database.
Deprecated. Specifies the name of the database on the master server to connect to. The connection is used only for managing the backup process, so you can connect to any existing database. Can be set in the pg_probackup.conf using the set-config command. 

    --master-host=host
Deprecated. Specifies the host name of the system on which the master server is running. 

    --master-port=port
    Default: 5432, the Postgres Pro default port. 
Deprecated. Specifies the TCP port or the local Unix domain socket file extension on which the master server is listening for connections. 

    --master-user=username
    Default: postgres, the Postgres Pro default user name.
Deprecated. User name to connect as. 

    --replica-timeout=timeout
    Default: 300 sec
Deprecated. Wait time for WAL segment streaming via replication, in seconds. By default, pg_probackup waits 300 seconds. You can also define this parameter in the pg_probackup.conf configuration file using the set-config command. 

**Archiving Options**

    --wal-file-path=wal_file_path %p
Provides the path to the WAL file in archive_command and restore_command used by pg_probackup. The %p variable is required for correct processing. 

    --wal-file-name=wal_file_name %f
Provides the name of the WAL file in archive_command and restore_command used by pg_probackup. The %f variable is required for correct processing. 

    --overwrite
Overwrites archived WAL file. Use this option together with the archive-push command if the specified subdirectory of the backup catalog already contains this WAL file and it needs to be replaced with its newer copy. Otherwise, archive-push reports that a WAL segment already exists, and aborts the operation. If the file to replace has not changed, archive-push skips this file regardless of the --overwrite option. 

**checkdb Options**

    --amcheck
Performs logical verification of indexes for the specified Postgres Pro instance if no corruption was found while checking data files. Optionally, you can skip validation of data files by specifying --skip-block-validation. You must have the amcheck extension installed in the database to check its indexes. For databases without amcheck, index verification will be skipped. 

    --heapallindexed
Checks that all heap tuples that should be indexed are actually indexed. You can use this option only together with the --amcheck option starting from Postgres Pro 11. 

**Remote Backup Options**
This section describes the options related to running backup and restore operations remotely via SSH. These options can be used with add-instance, set-config, backup, restore, archive-push, and archive-get commands.

    --remote-proto
Specifies the protocol to use for remote operations. Currently only the SSH protocol is supported. Possible values are:
- ssh enables the remote backup mode via SSH.
- none explicitly disables the remote backup mode. 

You can omit this option if the --remote-host option is specified. 

    --remote-host
Specifies the remote host IP address or hostname to connect to. 

    --remote-port
    Default: 22 
Specifies the remote host port to connect to.

    --remote-user
Specifies remote host user for SSH connection. If you omit this option, the current user initiating the SSH connection is used. 

    --remote-path
Specifies pg_probackup installation directory on the remote system. 

    --ssh-options
Specifies a string of SSH command-line options. 

### Usage

- [Creating a Backup](#creating-a-backup)
- [Validating a Backup](#vaklidating-a-backup)
- [Restoring a Cluster](#restoting-a-cluster)
- [Using pg_probackup in the Remote Backup Mode](#using-pg_probackup-in-the-remote-backup-mode)
- [Running pg_probackup on Parallel Threads](#running-pg_probackup-on-parallel-threads)
- [Configuring pg_probackup](#configuring-pg_probackup)
- [Managing the Backup Catalog](#managing-the-backup-Catalog)

##### Creating a Backup
To create a backup, run the following command:

    pg_probackup backup -B backupdir --instance instance_name -b backup_mode
where backup_mode can take one of the following values:
- FULL — creates a full backup that contains all the data files of the cluster to be restored.
- DELTA — reads all data files in the data directory and creates an incremental backup for pages that have changed since the previous backup.
- PAGE — creates an incremental PAGE backup based on the WAL files that have changed since the previous full or incremental backup was taken.
- PTRACK — creates an incremental PTRACK backup tracking page changes on the fly. 

When restoring a cluster from an incremental backup, pg_probackup relies on the previous full backup to restore all the data files first. Thus, you must create at least one full backup before taking incremental ones.

If you have configured PTRACK backups, pg_probackup clears PTRACK bitmap of the relation being processed each time a full or an incremental backup is taken. Thus, the next incremental PTRACK backup contains only the pages that have changed since the previous backup. If a backup failed or was interrupted, some relations can already have their PTRACK forks cleared, so the next incremental backup will be incomplete. The same is true if ptrack_enable was turned off for some time. In this case, you must take a full backup before the next incremental PTRACK backup.

To make a backup autonomous, add the --stream option to the above command. For example, to create a full autonomous backup, run:

    pg_probackup backup -B backupdir --instance instance_name -b FULL --stream --temp-slot

The optional --temp-slot parameter ensures that the required segments remain available if the WAL is rotated before the backup is complete.

Autonomous backups include all the WAL segments required to restore the cluster to a consistent state at the time the backup was taken. To restore a cluster from an incremental autonomous backup, pg_probackup still requires the full backup and all the incremental backups it depends on.

Even if you are using continuous archiving, autonomous backups can still be useful in the following cases:

    Autonomous backups can be restored on the server that has no file access to WAL archive.

    Autonomous backups enable you to restore the cluster state at the point in time for which WAL files are no longer available. 

To back up a directory located outside of the data directory, use the optional --external-dirs parameter that specifies the path to this directory. If you would like to add more than one external directory, provide several paths separated by colons. For example, to include /etc/dir1/ and /etc/dir2/ directories into the full backup of your node instance that will be stored under the node_backup directory, run:

    pg_probackup backup -B node_backup --instance node -b FULL --external-dirs=/etc/dir1:/etc/dir2

pg_probackup creates a separate subdirectory in the backup directory for each external directory. Since external directories included into different backups do not have to be the same, when you are restoring the cluster from an incremental backup, only those directories that belong to this particular backup will be restored. Any external directories stored in the previous backups will be ignored. To include the same directories into each backup of your instance, you can specify them in the pg_probackup.conf configuration file using the set-config command with the --external-dirs option.

##### Validating Backups
When checksums are enabled for the database cluster, pg_probackup uses this information to check correctness of data files. While reading each page, pg_probackup checks whether the calculated checksum coincides with the checksum stored in the page. This guarantees that the backup is free of corrupted pages. Note that pg_probackup reads database files from disk and under heavy write load during backup it can show false positive checksum failures because of partial writes.

Even if page checksums are disabled, pg_probackup calculates checksums for each file in a backup. Checksums are checked immediately after backup is taken and right before restore, to detect possible backup corruptions. If you would like to skip backup validation, you can specify the --no-validate option when running backup and restore commands.

To ensure that all the required backup files are present and can be used to restore the database cluster, you can run the validate command with the exact recovery target options you are going to use for recovery. If you omit all the parameters, all backups are validated.

For example, to check that you can restore the database cluster from a backup copy up to the specified xid transaction ID, run this command:

    pg_probackup validate -B backupdir --instance instance_name --recovery-target-xid=xid

If validation completes successfully, pg_probackup displays the corresponding message. If validation fails, you will receive an error message with the exact time and transaction ID up to which the recovery is possible.

##### Restoring a Cluster
To restore the database cluster from a backup, run the restore command with at least the following options:

    pg_probackup restore -B backupdir --instance instance_name -i backup_id

where:
- backupdir is the backup catalog that stores all backup files and meta information.
- instance_name is the backup instance for the cluster to be restored.
- backup_id specifies the backup to restore the cluster from. If you omit this option, pg_probackup uses the latest backup available for the specified instance. If you specify an incremental backup to restore, pg_probackup automatically restores the underlying full backup and then sequentially applies all the necessary increments. 

If the cluster to restore contains tablespaces, pg_probackup restores them to their original location by default. To restore tablespaces to a different location, use the --tablespace-mapping option. Otherwise, restoring the cluster on the same host will fail if tablespaces are in use, because the backup would have to be written to the same directories.

When using the --tablespace-mapping option, you must provide absolute paths to the old and new tablespace directories. If a path happens to contain an equals sign (=), escape it with a backslash. This option can be specified multiple times for multiple tablespaces. For example:

    pg_probackup restore -B backupdir --instance instance_name -D datadir -j 4 -i backup_id -T tablespace1_dir=tablespace1_newdir -T tablespace2_dir=tablespace2_newdir

Once the restore command is complete, start the database service. If you are restoring an autonomous backup, the restore is complete at once, with the cluster returned to a self-consistent state at the point when the backup was taken. For archive backups, Postgres Pro replays all archived WAL segments, so the cluster is restored to the latest state possible. You can change this behavior by using the recovery_target option with the restore command. Note that using the recovery-target=latest value with autonomous backups is only possible if the WAL archive is available at least starting from the time the autonomous backup was taken.

>NOTE: By default, the restore command validates the specified backup before restoring the cluster. If you run regular backup validations and would like to save time when restoring the cluster, you can specify the --no-validate option to skip validation and speed up the recovery.

**Performing Point-in-Time (PITR) Recovery**
If you have enabled continuous WAL archiving before taking backups, you can restore the cluster to its state at an arbitrary point in time (recovery target) using recovery target options with the restore command instead of the -i option shown above. pg_probackup automatically chooses the backup that is the closest to the specified recovery target and starts the recovery process.

- To restore the cluster state at the exact time, specify the recovery-target-time option, in the timestamp format. For example:

        pg_probackup restore -B backupdir --instance instance_name --recovery-target-time='2017-05-18 14:18:11'

- To restore the cluster state up to a specific transaction ID, use the recovery-target-xid option:

        pg_probackup restore -B backupdir --instance instance_name --recovery-target-xid=687
- If you know the exact LSN up to which you need to restore the data, use recovery-target-lsn:

        pg_probackup restore -B backupdir --instance instance_name --recovery-target-lsn=16/B374D848

By default, the recovery_target_inclusive parameter defines whether the recovery target is included into the backup. You can explicitly include or exclude the recovery target by adding the --recovery-target-inclusive=boolean option to the commands listed above.

##### Using pg_probackup in the Remote Backup Mode

pg_probackup supports the remote backup mode that allows to perform backup and restore operations remotely via SSH. In this mode, the backup catalog is stored on a local system, while Postgres Pro instance to be backed up is located on a remote system. You must have pg_probackup installed on both systems.

The typical workflow is as follows:

 - On your local system, configure pg_probackup as explained in the section called “Installation and Setup”. For the add-instance and set-config commands, make sure to specify remote backup options that point to the remote server with the Postgres Pro instance.

- If you would like to take archive backups, configure continuous WAL archiving on the remote system as explained in the section called “Setting up Archive Backups”. For the archive-push and archive-get commands, you must specify the remote backup options that point to your local system.

- Run backup or restore commands with remote backup options on your local system. pg_probackup connects to the remote system via SSH and creates a backup locally or restores the previously taken backup on the remote system, respectively. 

>NOTE: The remote backup mode is currently unavailable for Windows systems.

##### Running pg_probackup on Parallel Threads

Backup, recovery, and validation processes can be executed on several parallel threads. This can significantly speed up pg_probackup operation given enough resources (CPU cores, disk, and network throughput).

Parallel execution is controlled by the -j/--threads command line option. For example, to create a backup using four parallel threads, run:

    pg_probackup backup -B backupdir --instance instance_name -b FULL -j 4

>NOTE: Parallel recovery applies only to copying data from the backup catalog to the data directory of the cluster. When Postgres Pro server is started, WAL records need to be replayed, and this cannot be done in parallel.

##### Configuring pg_probackup

Once the backup catalog is initialized and a new backup instance is added, you can use the pg_probackup.conf configuration file located in the backups/instance_name directory to fine-tune pg_probackup configuration.

Initially, pg_probackup.conf contains the following settings:
- PGDATA — the path to the data directory of the cluster to back up.
- system-identifier — the unique identifier of the Postgres Pro instance. 

Additionally, you can define connection, retention, logging, and replica settings using the set-config command:

    pg_probackup set-config -B backupdir --instance instance_name --external-dirs=external_directory_path [connection_options] [retention_options] [logging_options] [replica_options]

To view the current settings, run the following command:

    pg_probackup show-config -B backupdir --instance instance_name

You can override the settings defined in pg_probackup.conf when running the backup command.

**Specifying Connection Settings**

If you define connection settings in the pg_probackup.conf configuration file, you can omit connection options in all the subsequent pg_probackup commands. However, if the corresponding environment variables are set, they get higher priority. The options provided on the command line overwrite both environment variables and configuration file settings.

If nothing is given, the default values are taken. pg_probackup tries to use local connection and tries to get the database name and the user name from the PGUSER environment variable or the current OS user name.

**Configuring Backup Retention Policy**

By default, all backup copies created with pg_probackup are stored in the specified backup catalog. To save disk space, you can configure retention policy and periodically clean up redundant backup copies accordingly.

To configure retention policy, set one or more of the following variables in the pg_probackup.conf file:
- retention-redundancy — specifies the number of full backup copies to keep in the backup catalog.
- retention-window — defines the earliest point in time for which pg_probackup can complete the recovery. This option is set in the number of days from the current moment. For example, if retention-window=7, pg_probackup must keep at least one full backup copy that is older than seven days, with all the corresponding WAL files.

If both retention-redundancy and retention-window options are set, pg_probackup keeps backup copies that satisfy both conditions. For example, if you set retention-redundancy=2 and retention-window=7, pg_probackup cleans up the backup directory to keep only two full backup copies if at least one of them is older than seven days.

To clean up the backup catalog in accordance with retention policy, run:

    pg_probackup delete -B backupdir --instance instance_name --expired

pg_probackup deletes all backup copies that do not conform to the defined retention policy.

If you would like to also remove the WAL files that are no longer required for any of the backups, add the --wal option:

    pg_probackup delete -B backupdir --instance instance_name --expired --wal

Alternatively, you can use the --delete-expired and --delete-wal options together with the backup command to remove the outdated backup copies once the new backup is created.

Since incremental backups require that their parent full backup and all the preceding incremental backups are available, if any of such backups expire, they still cannot be removed while at least one incremental backup in this chain satisfies the retention policy. To avoid keeping expired backups that are still required to restore an active incremental one, you can merge them with this backup using the --merge-expired option when running backup or delete commands.

Suppose you have backed up the node instance in the node-backup directory, with the retention-window option is set to 7, and you have the following backups available on April 10, 2019:

```
BACKUP INSTANCE 'node'
===========================================================================================================================================
 Instance    Version  ID      Recovery time           Mode    WAL      Current/Parent TLI   Time   Data    Start LSN    Stop LSN    Status 
===========================================================================================================================================
 node        10       P7XDHR  2019-04-10 05:27:15+03  FULL    STREAM     1 / 0               11s   200MB   0/18000059   0/18000197  OK
 node        10       P7XDQV  2019-04-08 05:32:59+03  DELTA   STREAM     1 / 0               11s    19MB   0/15000060   0/15000198  OK
 node        10       P7XDJA  2019-04-03 05:28:36+03  PTRACK  STREAM     1 / 0               21s    32MB   0/13000028   0/13000198  OK
 node        10       P7XDHU  2019-04-02 05:27:59+03  PTRACK  STREAM     1 / 0               31s    33MB   0/11000028   0/110001D0  OK
 node        10       P7XDHB  2019-04-01 05:27:15+03  FULL    STREAM     1 / 0               11s   200MB   0/F000028    0/F000198   OK
 node        10       P7XDFT  2019-03-29 05:26:25+03  FULL    STREAM     1 / 0               11s   200MB   0/D000028    0/D000198   OK
```

Even though P7XDHB and P7XDHU backups are outside the retention window, they cannot be removed as it invalidates the succeeding incremental backups P7XDHU and P7XDQV that are still required, so if you run the delete command with the --expired option, only the P7XDFT full backup will be removed. With the --merge-expired option, the P7XDJA backup is merged with the underlying P7XDHB and P7XDHU backups and becomes a full one, so there is no need to keep these expired backups anymore:

    pg_probackup delete -B node-backup --instance node --expired --merge-expired
    pg_probackup show -B node-backup

```
BACKUP INSTANCE 'node'
============================================================================================================================================
 Instance    Version  ID      Recovery time           Mode    WAL      Current/Parent TLI    Time   Data    Start LSN    Stop LSN    Status
============================================================================================================================================
 node        10       P7XDHR  2019-04-10 05:27:15+03  FULL    STREAM     1 / 0                11s   200MB   0/18000059   0/18000197  OK
 node        10       P7XDQV  2019-04-08 05:32:59+03  DELTA   STREAM     1 / 0                11s    19MB   0/15000060   0/15000198  OK
 node        10       P7XDJA  2019-04-04 05:28:36+03  FULL    STREAM     1 / 0                 5s   200MB   0/13000028   0/13000198  OK
```

Note that the Time field for the merged backup displays the time required for the merge.

##### Managing the Backup Catalog

With pg_probackup, you can manage backups from the command line:

- View available backups
- Merge backups
- Delete backups 
- Viewing Backup Information

To view the list of existing backups, run the command:

    pg_probackup show -B backupdir

pg_probackup displays the list of all the available backups. For example:

```
BACKUP INSTANCE 'node'
============================================================================================================================================
 Instance    Version  ID      Recovery time           Mode    WAL      Current/Parent TLI    Time    Data   Start LSN    Stop LSN    Status 
============================================================================================================================================
 node        10       P7XDQV  2018-04-29 05:32:59+03  DELTA   STREAM     1 / 0                11s    19MB   0/15000060   0/15000198  OK
 node        10       P7XDJA  2018-04-29 05:28:36+03  PTRACK  STREAM     1 / 0                21s    32MB   0/13000028   0/13000198  OK
 node        10       P7XDHU  2018-04-29 05:27:59+03  PTRACK  STREAM     1 / 0                31s    33MB   0/11000028   0/110001D0  OK
 node        10       P7XDHB  2018-04-29 05:27:15+03  FULL    STREAM     1 / 0                11s    39MB   0/F000028    0/F000198   OK
 node        10       P7XDFT  2018-04-29 05:26:25+03  PTRACK  STREAM     1 / 0                11s    40MB   0/D000028    0/D000198   OK
```

For each backup, the following information is provided:
- Instance — the instance name.
- Version — Postgres Pro version.
- ID — the backup identifier.
- Recovery time — the earliest moment for which you can restore the state of the database cluster.
- Mode — the method used to take this backup. Possible values: FULL, PAGE, DELTA, PTRACK.
- WAL — the way of WAL log handling. Possible values: STREAM for autonomous backups and ARCHIVE for archive backups.
- Current/Parent TLI — current and parent timelines of the database cluster.
- Time — the time it took to perform the backup.
- Data — the size of the data files in this backup. This value does not include the size of WAL files.
- Start LSN — WAL log sequence number corresponding to the start of the backup process.
- Stop LSN — WAL log sequence number corresponding to the end of the backup process.
- Status — backup status. Possible values:
    - OK — the backup is complete and valid.
    - CORRUPT — some of the backup files are corrupted.
    - DONE — the backup is complete, but was not validated.
    - ERROR — the backup was aborted because of an unexpected error.
    - RUNNING — the backup is in progress.
    - MERGING — the backup is being merged.
    - ORPHAN — the backup is invalid because one of its parent backups is corrupt.
    - DELETING — the backup files are being deleted.

You can restore the cluster from the backup only if the backup status is OK. 

To get more detailed information about the backup, run the show with the backup ID:

    pg_probackup show -B backupdir --instance instance_name -i backup_id

The sample output is as follows:
>#Configuration
backup-mode = FULL
stream = false
#Compatibility
block-size = 8192
wal-block-size = 8192
checksum-version = 0
#Result backup info
timelineid = 1
start-lsn = 0/04000028
stop-lsn = 0/040000f8
start-time = '2017-05-16 12:57:29'
end-time = '2017-05-16 12:57:31'
recovery-xid = 597
recovery-time = '2017-05-16 12:57:31'
data-bytes = 22288792
status = OK

**Merging Backups**

As you take more and more incremental backups, the total size of the backup catalog can substantially grow. To save disk space, you can merge incremental backups to their parent full backup by running the merge command, specifying the backup ID of the most recent incremental backup you would like to merge:

    pg_probackup merge -B backupdir --instance instance_name -i backup_id

This command merges the specified incremental backup to its parent full backup, together with all incremental backups between them. Once the merge is complete, the incremental backups are removed as redundant. Thus, the merge operation is virtually equivalent to retaking a full backup and removing all the outdated backups, but it allows to save much time, especially for large data volumes.

Before the merge, pg_probackup validates all the affected backups to ensure that they are valid. You can check the current backup status by running the show command with the backup ID. If the merge is still in progress, the backup status is displayed as MERGING. You can restart the merge if it is interrupted.
Deleting Backups

To delete a backup that is no longer required, run the following command:

    pg_probackup delete -B backupdir --instance instance_name -i backup_id

This command will delete the backup with the specified backup_id, together with all the incremental backups that followed, if any. This way, you can delete some recent incremental backups, retaining the underlying full backup and some of the incremental backups that follow it.
In this case, the next PTRACK backup will be incomplete as some changes since the last retained backup will be lost. Either a full backup or an incremental PAGE backup (if all the necessary WAL files are still present in the archive) must be taken then.

To delete obsolete WAL files that are not necessary to restore any of the remaining backups, use the --wal option:

    pg_probackup delete -B backupdir --instance instance_name --wal

To delete backups that are expired according to the current retention policy, use the --expired option:

    pg_probackup delete -B backupdir --instance instance_name --expired

Note that expired backups cannot be removed while at least one incremental backup that satisfies the retention policy is based on them. If you would like to minimize the number of backups still required to keep incremental backups valid, specify the --merge-expired option when running this command:

    pg_probackup delete -B backupdir --instance instance_name --expired --merge-expired

In this case, pg_probackup searches for the oldest incremental backup that satisfies the retention policy and merges this backup with the underlying full and incremental backups that have already expired, thus making it a full backup. Once the merge is complete, the remaining expired backups are deleted.

Before merging or deleting backups, you can run the delete command with the dry-run option, which displays the status of all the available backups according to the current retention policy, without performing any irreversible actions.

### Authors
Postgres Professional, Moscow, Russia.
### Credits
pg_probackup utility is based on pg_arman, that was originally written by NTT and then developed and maintained by Michael Paquier. 