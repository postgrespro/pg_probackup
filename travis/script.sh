#!/usr/bin/env bash

set -xe

export PGHOME=/pg
export PG_SRC=$PWD/postgres
export PATH=$PGHOME/bin:$PATH
export LD_LIBRARY_PATH=$PGHOME/lib
export PG_CONFIG=$(which pg_config)

# Build and install pg_probackup (using PG_CPPFLAGS and SHLIB_LINK for gcov)
echo "############### Compiling and installing pg_probackup:"
# make USE_PGXS=1 PG_CPPFLAGS="-coverage" SHLIB_LINK="-coverage" top_srcdir=$CUSTOM_PG_SRC install
make USE_PGXS=1 top_srcdir=$PG_SRC install

if [ -z ${MODE+x} ]; then
	MODE=basic
fi

if [ -z ${PGPROBACKUP_GDB+x} ]; then
	PGPROBACKUP_GDB=ON
fi

if [ -z ${PG_PROBACKUP_PTRACK+x} ]; then
	PG_PROBACKUP_PTRACK=ON
fi

echo "############### Testing:"
echo PG_PROBACKUP_PARANOIA=${PG_PROBACKUP_PARANOIA}
echo ARCHIVE_COMPRESSION=${ARCHIVE_COMPRESSION}
echo PGPROBACKUPBIN_OLD=${PGPROBACKUPBIN_OLD}
echo PGPROBACKUPBIN=${PGPROBACKUPBIN}
echo PGPROBACKUP_SSH_REMOTE=${PGPROBACKUP_SSH_REMOTE}
echo PGPROBACKUP_GDB=${PGPROBACKUP_GDB}
echo PG_PROBACKUP_PTRACK=${PG_PROBACKUP_PTRACK}

#Run Full tests only if FULL_TESTS=ON e.g. for master branch
if [ "$MODE" = "full" ] && [ -z ${FULL_TESTS} ]; then
    exit
fi

if [ "$MODE" = "basic" ]; then
    export PG_PROBACKUP_TEST_BASIC=ON
    echo PG_PROBACKUP_TEST_BASIC=${PG_PROBACKUP_TEST_BASIC}
    python3 -m pytest -v -n4 -k "test_basic"
    python3 -m pytest -v -n4 -k "init_test.py"
elif [ "$MODE" = "full" ]; then
    echo PG_PROBACKUP_TEST_BASIC=${PG_PROBACKUP_TEST_BASIC}
    python3 -m pytest -v -n4
else
    echo PG_PROBACKUP_TEST_BASIC=${PG_PROBACKUP_TEST_BASIC}
    python3 -m pytest -v -n4 -k "$MODE"
fi
