pg_arman
========

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
https://github.com/michaelpq/pg_arman.

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

License
-------

pg_arman can be distributed under the PostgreSQL license. See COPYRIGHT
file for more information. pg_arman is a fork of the existing project
pg_rman, initially created and maintained by NTT and Itagaki Takahiro.
