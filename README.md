pg_arman fork from Postgres Professional
========================================

This repository contains fork of pg_arman by Postgres Professional with
block level incremental backup support.

pg_arman is a backup and recovery manager for PostgreSQL servers able to do
differential and full backup as well as restore a cluster to a
state defined by a given recovery target. It is designed to perform
periodic backups of an existing PostgreSQL server, combined with WAL
archives to provide a way to recover a server in case of failure of
server because of a reason or another. Its differential backup
facility reduces the amount of data necessary to be taken between
two consecutive backups.

Download
--------

The latest version of this software can be found on the project website at
https://github.com/postgrespro/pg_arman.  Original fork of pg_arman can be
found at https://github.com/michaelpq/pg_arman.

Installation
------------

Compiling pg_arman requires a PostgreSQL installation to be in place
as well as a raw source tree. Pass the path to the PostgreSQL source tree
to make, in the top_srcdir variable:

    make USE_PGXS=1 top_srcdir=<path to PostgreSQL source tree>

In addition, you must have pg_config in $PATH.

The current version of pg_arman is compatible with PostgreSQL 9.5 and
upper versions.

Platforms
---------

pg_arman has been tested on Linux and Unix-based platforms.

Documentation
-------------

All the documentation you can find [here](doc/pg_arman.md).

Regression tests
----------------

The test suite of pg_arman is available in the code tree and can be
launched in a way similar to common PostgreSQL extensions and modules:

    make installcheck

Block level incremental backup
------------------------------

Idea of block level incremental backup is that you may backup only blocks
changed since last full backup.  It gives two major benefits: taking backups
faster and making backups smaller.

The major question here is how to get the list of changed blocks.  Since
each block contains LSN number, changed blocks could be retrieved by full scan
of all the blocks.  But this approach consumes as much server IO as full
backup.

This is why we implemented alternative approaches to retrieve
list of changed blocks.

1. Scan WAL archive and extract changed blocks from it.  However, shortcoming
of these approach is requirement to have WAL archive.

2. Track bitmap of changes blocks inside PostgreSQL (ptrack).  It introduces
some overhead to PostgreSQL performance.  On our experiments it appears to be
less than 3%.

These two approaches were implemented in this fork of pg_arman.  The second
approach requires [patch for PostgreSQL 9.5](https://gist.github.com/stalkerg/44703dbcbac1da08f448b7e6966646c0).

Testing block level incremental backup
--------------------------------------

You need build and install [PGPRO9_5 branch of PostgreSQL](https://github.com/postgrespro/postgrespro) or [apply this patch to PostgreSQL 9.5](https://gist.github.com/stalkerg/44703dbcbac1da08f448b7e6966646c0).

### Retrieving changed blocks from WAL archive

You need to enable WAL archive by adding following lines to postgresql.conf:

```
wal_level = archive
archive_command = 'test ! -f /home/postgres/backup/arman/wal/%f && cp %p /home/postgres/backup/arman/wal/%f'
wal_log_hints = on
```

Example backup (assuming PostgreSQL is running):
```bash
# Init pg_aramn backup folder
pg_arman init -B /home/postgres/backup/pgarman
cat << __EOF__ >> /home/postgres/backup/pgarman/pg_arman.ini
ARCLOG_PATH = '/home/postgres/backup/arman/wal'
__EOF__
# Make full backup with 2 thread and verbose mode.
pg_arman backup -B /home/postgres/backup/pgarman -D /home/postgres/pgdata/arman -b full -v -j 2
# Validate backup
pg_arman validate -B /home/postgres/backup/pgarman -D /home/postgres/pgdata/arman
# Show backups information
pg_arman show -B /home/postgres/backup/pgarman

# Now you can insert or update some data in your database

# Then start the incremental backup.
pg_arman backup -B /home/postgres/backup/pgarman -D /home/postgres/pgdata/arman -b page -v -j 2
pg_arman validate -B /home/postgres/backup/pgarman -D /home/postgres/pgdata/arman
# You should see that increment is really small
pg_arman show -B /home/postgres/backup/pgarman
```

For restore after remove your pgdata you can use:
```
pg_arman restore -B /home/postgres/backup/pgarman -D /home/postgres/pgdata/arman -j 4 --verbose
```

### Retrieving changed blocks from ptrack

The advantage of this approach is that you don't have to save WAL archive.  You will need to enable ptrack in postgresql.conf (restart required).

```
ptrack_enable = on
```

Also, some WALs still need to be fetched in order to get consistent backup.  pg_arman can fetch them trough the streaming replication protocol.  Thus, you also need to [enable streaming replication connection](https://wiki.postgresql.org/wiki/Streaming_Replication).

Example backup (assuming PostgreSQL is running):
```bash
# Init pg_aramn backup folder
pg_arman init -B /home/postgres/backup/pgarman
cat << __EOF__ >> /home/postgres/backup/pgarman/pg_arman.ini
ARCLOG_PATH = '/home/postgres/backup/arman/wal'
__EOF__
# Make full backup with 2 thread and verbose mode.
pg_arman backup -B /home/postgres/backup/pgarman -D /home/postgres/pgdata/arman -b full -v -j 2 --stream
# Validate backup
pg_arman validate -B /home/postgres/backup/pgarman -D /home/postgres/pgdata/arman
# Show backups information
pg_arman show -B /home/postgres/backup/pgarman

# Now you can insert or update some data in your database

# Then start the incremental backup.
pg_arman backup -B /home/postgres/backup/pgarman -D /home/postgres/pgdata/arman -b ptrack -v -j 2 --stream
pg_arman validate -B /home/postgres/backup/pgarman -D /home/postgres/pgdata/arman
# You should see that increment is really small
pg_arman show -B /home/postgres/backup/pgarman
```

For restore after remove your pgdata you can use:
```
pg_arman restore -B /home/postgres/backup/pgarman -D /home/postgres/pgdata/arman -j 4 --verbose --stream
```

License
-------

pg_arman can be distributed under the PostgreSQL license. See COPYRIGHT
file for more information. pg_arman is a fork of the existing project
pg_rman, initially created and maintained by NTT and Itagaki Takahiro.
