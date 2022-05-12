#!/usr/bin/env sh

set -x

if [ -z ${PG_VERSION+x} ]; then
	echo PG_VERSION is not set!
	exit 1
fi

if [ -z ${PG_BRANCH+x} ]; then
	echo PG_BRANCH is not set!
	exit 1
fi

if [ -z ${PTRACK_PATCH_PG_BRANCH+x} ]; then
	PTRACK_PATCH_PG_BRANCH=OFF
fi

export PG_SRC=$PWD/postgres

# Clone Postgres
echo "############### Getting Postgres sources..."
git clone https://github.com/postgres/postgres.git -b $PG_BRANCH --depth=1

# Clone ptrack
if [ "$PTRACK_PATCH_PG_BRANCH" != "OFF" ]; then
    git clone https://github.com/postgrespro/ptrack.git -b master --depth=1 postgres/contrib/ptrack
    export PG_PROBACKUP_PTRACK=ON
else
    export PG_PROBACKUP_PTRACK=OFF
fi

cd postgres
# Compile and install Postgres
echo "############### Compiling Postgres..."
if [ "$PG_PROBACKUP_PTRACK" = "ON" ]; then
    git apply -3 contrib/ptrack/patches/${PTRACK_PATCH_PG_BRANCH}-ptrack-core.diff
fi
CFLAGS="-Og" ./configure --prefix=$PG_BASE \
    --enable-debug --enable-cassert --enable-depend \
    --enable-tap-tests --enable-nls
make -s -j$(nproc) install
make -s -j$(nproc) -C contrib/ install

export PATH=$PATH:$PG_BASE/bin
export LD_LIBRARY_PATH=$PG_BASE/lib

if [ "$PG_PROBACKUP_PTRACK" = "ON" ]; then
    echo "############### Compiling Ptrack..."
    make -C contrib/ptrack install
fi

# Get amcheck if missing
if [ ! -d "contrib/amcheck" ]; then
    echo "############### Getting missing amcheck..."
    git clone https://github.com/petergeoghegan/amcheck.git --depth=1 contrib/amcheck
    make -C contrib/amcheck install
fi

cd ..
# Build and install pg_probackup (using PG_CPPFLAGS and SHLIB_LINK for gcov)
echo "############### Compiling and installing pg_probackup..."
# make USE_PGXS=1 PG_CPPFLAGS="-coverage" SHLIB_LINK="-coverage" top_srcdir=$CUSTOM_PG_SRC install
make install USE_PGXS=1 top_srcdir=$PG_SRC PG_CONFIG=$PG_BASE/bin/pg_config

