pg_probackup fork of pg_arman by Postgres Professional
========================================

pg_probackup is a backup and recovery manager for PostgreSQL servers able to do
differential and full backup as well as restore a cluster to a
state defined by a given recovery target. It is designed to perform
periodic backups of an existing PostgreSQL server, combined with WAL
archives to provide a way to recover a server in case of failure of
server because of a reason or another. Its differential backup
facility reduces the amount of data necessary to be taken between
two consecutive backups.

Main features:
* incremental backup from WAL and PTRACK
* backup from replica
* multithreaded backup and restore
* autonomous backup without archive command (will need slot replication)

Requirements:
* >=PostgreSQL 9.5
* >=gcc 4.4 or >=clang 3.6 or >= XLC 12.1
* pthread

Download
--------

The latest version of this software can be found on the project website at
https://github.com/postgrespro/pg_probackup.  Original fork of pg_probackup can be
found at https://github.com/michaelpq/pg_arman.

Installation
------------

Compiling pg_probackup requires a PostgreSQL installation to be in place
as well as a raw source tree. Pass the path to the PostgreSQL source tree
to make, in the top_srcdir variable:

    make USE_PGXS=1 top_srcdir=<path to PostgreSQL source tree>

In addition, you must have pg_config in $PATH.

The current version of pg_probackup is compatible with PostgreSQL 9.5 and
upper versions.

Platforms
---------

pg_probackup has been tested on Linux and Unix-based platforms.

Documentation
-------------

All the documentation you can find [here](doc/pg_probackup.md).

Regression tests
----------------

For tests you must have python 2.7 or python 3.3 and higher. Also good idea
is make virtual enviroment by `virtualenv`.   
First of all you need to install `testgres` python module which contains useful
functions to start postgres clusters and make queries:

```
pip install testgres
```

To run tests execute:

```
python -m unittest tests
```

from current (root of project) directory. If you want to run a specific postgres build then
you should specify the path to your pg_config executable by setting PG_CONFIG
environment variable:
```
export PG_CONFIG=/path/to/pg_config
```


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

These two approaches were implemented in this fork of pg_probackup.  The second
approach requires [patch for PostgreSQL 9.5](https://gist.github.com/stalkerg/44703dbcbac1da08f448b7e6966646c0) or
[patch for PostgreSQL 10](https://gist.github.com/stalkerg/ab833d94e2f64df241f1835651e06e4b).

Testing block level incremental backup
--------------------------------------

You need build and install [PGPRO9_5 or PGPRO9_6 branch of PostgreSQL](https://github.com/postgrespro/postgrespro) or apply this patch to
[PostgreSQL 9.5](https://gist.github.com/stalkerg/44703dbcbac1da08f448b7e6966646c0) or [PostgreSQL 10](https://gist.github.com/stalkerg/ab833d94e2f64df241f1835651e06e4b).

### Retrieving changed blocks from WAL archive

You need to enable WAL archive by adding following lines to postgresql.conf:

```
wal_level = archive
archive_mode = on
archive_command = 'test ! -f /home/postgres/backup/wal/%f && cp %p /home/postgres/backup/wal/%f'
```

Example backup (assuming PostgreSQL is running):
```bash
# Init pg_aramn backup folder
pg_probackup init -B /home/postgres/backup
# Make full backup with 2 thread and verbose mode.
pg_probackup backup -B /home/postgres/backup -D /home/postgres/pgdata -b full -v -j 2
# Show backups information
pg_probackup show -B /home/postgres/backup

# Now you can insert or update some data in your database

# Then start the incremental backup.
pg_probackup backup -B /home/postgres/backup -D /home/postgres/pgdata -b page -v -j 2
# You should see that increment is really small
pg_probackup show -B /home/postgres/backup
```

For restore after remove your pgdata you can use:
```
pg_probackup restore -B /home/postgres/backup -D /home/postgres/pgdata -j 4 --verbose
```

### Retrieving changed blocks from ptrack

The advantage of this approach is that you don't have to save WAL archive.  You will need to enable ptrack in postgresql.conf (restart required).

```
ptrack_enable = on
```

Also, some WALs still need to be fetched in order to get consistent backup.  pg_probackup can fetch them trough the streaming replication protocol.  Thus, you also need to [enable streaming replication connection](https://wiki.postgresql.org/wiki/Streaming_Replication).

Example backup (assuming PostgreSQL is running):
```bash
# Init pg_aramn backup folder
pg_probackup init -B /home/postgres/backup
# Make full backup with 2 thread and verbose mode.
pg_probackup backup -B /home/postgres/backup -D /home/postgres/pgdata -b full -v -j 2 --stream
# Show backups information
pg_probackup show -B /home/postgres/backup

# Now you can insert or update some data in your database

# Then start the incremental backup.
pg_probackup backup -B /home/postgres/backup -D /home/postgres/pgdata -b ptrack -v -j 2 --stream
# You should see that increment is really small
pg_probackup show -B /home/postgres/backup
```

For restore after remove your pgdata you can use:
```
pg_probackup restore -B /home/postgres/backup -D /home/postgres/pgdata -j 4 --verbose --stream
```

License
-------

pg_probackup can be distributed under the PostgreSQL license. See COPYRIGHT
file for more information. pg_arman is a fork of the existing project
pg_rman, initially created and maintained by NTT and Itagaki Takahiro.
