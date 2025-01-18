[![GitHub release](https://img.shields.io/github/v/release/postgrespro/pg_probackup?include_prereleases)](https://github.com/postgrespro/pg_probackup/releases/latest)
[![Build Status](https://travis-ci.com/postgrespro/pg_probackup.svg?branch=master)](https://travis-ci.com/postgrespro/pg_probackup)

# pg_probackup

`pg_probackup` is a utility to manage backup and recovery of PostgreSQL database clusters. It is designed to perform periodic backups of the PostgreSQL instance that enable you to restore the server in case of a failure.

The utility is compatible with:
* PostgreSQL 11, 12, 13, 14, 15, 16, 17

As compared to other backup solutions, `pg_probackup` offers the following benefits that can help you implement different backup strategies and deal with large amounts of data:
* Incremental backup: page-level incremental backup allows you to save disk space, speed up backup and restore. With three different incremental modes, you can plan the backup strategy in accordance with your data flow.
* Incremental restore: page-level incremental restore allows you dramatically speed up restore by reusing valid unchanged pages in destination directory.
* Merge: using this feature allows you to implement "incrementally updated backups" strategy, eliminating the need to do periodical full backups.
* Validation: automatic data consistency checks and on-demand backup validation without actual data recovery
* Verification: on-demand verification of PostgreSQL instance with the `checkdb` command.
* Retention: managing WAL archive and backups in accordance with retention policy. You can configure retention policy based on recovery time or the number of backups to keep, as well as specify `time to live` (TTL) for a particular backup. Expired backups can be merged or deleted.
* Parallelization: running backup, restore, merge, delete, verificaton and validation processes on multiple parallel threads
* Compression: storing backup data in a compressed state to save disk space
* Deduplication: saving disk space by not copying unchanged non-data files, such as `_vm` or `_fsm`
* Remote operations: backing up PostgreSQL instance located on a remote system or restoring a backup remotely
* Backup from standby: avoid extra load on master by taking backups from a standby server
* External directories: backing up files and directories located outside of the PostgreSQL `data directory` (PGDATA), such as scripts, configuration files, logs, or SQL dump files.
* Backup Catalog: get list of backups and corresponding meta information in plain text or JSON formats
* Archive catalog: getting the list of all WAL timelines and the corresponding meta information in plain text or JSON formats
* Partial Restore: restore only the specified databases or exclude the specified databases from restore.

To manage backup data, `pg_probackup` creates a backup catalog. This directory stores all backup files with additional meta information, as well as WAL archives required for [point-in-time recovery](https://postgrespro.com/docs/postgresql/current/continuous-archiving.html). You can store backups for different instances in separate subdirectories of a single backup catalog.

Using `pg_probackup`, you can take full or incremental backups:
* `Full` backups contain all the data files required to restore the database cluster from scratch.
* `Incremental` backups only store the data that has changed since the previous backup. It allows to decrease the backup size and speed up backup operations. `pg_probackup` supports the following modes of incremental backups:
  * `PAGE` backup. In this mode, `pg_probackup` scans all WAL files in the archive from the moment the previous full or incremental backup was taken. Newly created backups contain only the pages that were mentioned in WAL records. This requires all the WAL files since the previous backup to be present in the WAL archive. If the size of these files is comparable to the total size of the database cluster files, speedup is smaller, but the backup still takes less space.
  * `DELTA` backup. In this mode, `pg_probackup` read all data files in PGDATA directory and only those pages, that where changed since previous backup, are copied. Continuous archiving is not necessary for it to operate. Also this mode could impose read-only I/O pressure equal to `Full` backup.
  * `PTRACK` backup. In this mode, PostgreSQL tracks page changes on the fly. Continuous archiving is not necessary for it to operate. Each time a relation page is updated, this page is marked in a special `PTRACK` bitmap for this relation. As one page requires just one bit in the `PTRACK` fork, such bitmaps are quite small. Tracking implies some minor overhead on the database server operation, but speeds up incremental backups significantly.

Regardless of the chosen backup type, all backups taken with `pg_probackup` support the following strategies of WAL delivery:
* `Autonomous backups` streams via replication protocol all the WAL files required to restore the cluster to a consistent state at the time the backup was taken. Even if continuous archiving is not set up, the required WAL segments are included into the backup.
* `Archive backups` rely on continuous archiving.

## ptrack support

`PTRACK` backup support provided via following options:
* vanilla PostgreSQL 11, 12, 13, 14, 15, 16, 17 with [ptrack extension](https://github.com/postgrespro/ptrack)
* Postgres Pro Standard 11, 12, 13, 14, 15, 16, 17
* Postgres Pro Enterprise 11, 12, 13, 14, 15, 16, 17

## Limitations

`pg_probackup` currently has the following limitations:
* The server from which the backup was taken and the restored server must be compatible by the [block_size](https://postgrespro.com/docs/postgresql/current/runtime-config-preset#GUC-BLOCK-SIZE) and [wal_block_size](https://postgrespro.com/docs/postgresql/current/runtime-config-preset#GUC-WAL-BLOCK-SIZE) parameters and have the same major release number.
* Remote backup via ssh on Windows currently is not supported.
* When running remote operations via ssh, remote and local pg_probackup versions must be the same.

## Documentation

Documentation can be found at [github](https://postgrespro.github.io/pg_probackup) and [Postgres Professional documentation](https://postgrespro.com/docs/postgrespro/current/app-pgprobackup)

## Development

* Stable version state can be found under the respective [release tag](https://github.com/postgrespro/pg_probackup/releases).
* `master` branch contains minor fixes that are planned to the nearest minor release.
* Upcoming major release is developed in a release branch i.e. `release_2_6`.

For detailed release plans check [Milestones](https://github.com/postgrespro/pg_probackup/milestones)

## Installation and Setup
### Windows Installation
Installers are available in release **assets**. [Latests](https://github.com/postgrespro/pg_probackup/releases/latest).

### Linux Installation

See the [Installation](https://postgrespro.github.io/pg_probackup/#pbk-install) section in the documentation.

Once you have `pg_probackup` installed, complete [the setup](https://postgrespro.github.io/pg_probackup/#pbk-setup).

For users of Postgres Pro products, commercial editions of pg_probackup are available for installation from the corresponding Postgres Pro product repository.

## Building from source
### Linux

To compile `pg_probackup`, you must have a PostgreSQL installation and raw source tree. Execute this in the module's directory:

```shell
make USE_PGXS=1 PG_CONFIG=<path_to_pg_config> top_srcdir=<path_to_PostgreSQL_source_tree>
```

The alternative way, without using the PGXS infrastructure, is to place `pg_probackup` source directory into `contrib` directory and build it there. Example:

```shell
cd <path_to_PostgreSQL_source_tree> && git clone https://github.com/postgrespro/pg_probackup contrib/pg_probackup && cd contrib/pg_probackup && make
```

### Windows

Currently pg_probackup can be build using only MSVC 2013.
Build PostgreSQL using [pgwininstall](https://github.com/postgrespro/pgwininstall) or [PostgreSQL instruction](https://www.postgresql.org/docs/current/install-windows-full.html) with MSVC 2013.
If zlib support is needed, src/tools/msvc/config.pl must contain path to directory with compiled zlib. [Example](https://gist.githubusercontent.com/gsmol/80989f976ce9584824ae3b1bfb00bd87/raw/240032950d4ac4801a79625dd00c8f5d4ed1180c/gistfile1.txt)

```shell
CALL "C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall" amd64
SET PATH=%PATH%;C:\Perl64\bin
SET PATH=%PATH%;C:\msys64\usr\bin
gen_probackup_project.pl C:\path_to_postgresql_source_tree
```

## License

This module available under the [license](LICENSE) similar to [PostgreSQL](https://www.postgresql.org/about/license/).

## Feedback

Do not hesitate to post your issues, questions and new ideas at the [issues](https://github.com/postgrespro/pg_probackup/issues) page.

## Authors

Postgres Professional, Moscow, Russia.

## Credits

`pg_probackup` utility is based on `pg_arman`, that was originally written by NTT and then developed and maintained by Michael Paquier.


### Localization files (*.po)

Description of how to add new translation languages.
1. Add a flag --enable-nls in configure.
2. Build postgres.
3. Adding to nls.mk in folder pg_probackup required files in GETTEXT_FILES.
4. In folder pg_probackup do 'make update-po'.
5. As a result, the progname.pot file will be created. Copy the content and add it to the file with the desired language.
6. Adding to nls.mk in folder pg_probackup required language in AVAIL_LANGUAGES.

For more information, follow the link below:
https://postgrespro.ru/docs/postgresql/12/nls-translator
