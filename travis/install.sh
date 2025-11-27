#!/usr/bin/env bash

set -xe

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

# fix
sudo chown -R travis /home/travis/.ccache

export PGHOME=/pg

# Clone Postgres
echo "############### Getting Postgres sources:"
git clone https://github.com/postgres/postgres.git -b $PG_BRANCH --depth=1

# Clone ptrack
if [ "$PTRACK_PATCH_PG_BRANCH" != "OFF" ]; then
    git clone https://github.com/postgrespro/ptrack.git -b master --depth=1 postgres/contrib/ptrack
    export PG_PROBACKUP_PTRACK=ON
else
    export PG_PROBACKUP_PTRACK=OFF
fi

# Compile and install Postgres
echo "############### Compiling Postgres:"
cd postgres # Go to postgres dir
if [ "$PG_PROBACKUP_PTRACK" = "ON" ]; then
    git apply -3 contrib/ptrack/patches/${PTRACK_PATCH_PG_BRANCH}-ptrack-core.diff
fi

if [ "$PG_BRANCH" = "REL_18_STABLE" ]; then
    git apply -3 ../patches/${PG_BRANCH}_pg_probackup.patch
fi

CC='ccache gcc' CFLAGS="-Og" ./configure --prefix=$PGHOME \
    --cache-file=~/.ccache/configure-cache \
    --enable-debug --enable-cassert --enable-depend \
    --enable-tap-tests --enable-nls
make -s -j$(nproc) install
make -s -j$(nproc) -C contrib/ install

# Override default Postgres instance
export PATH=$PGHOME/bin:$PATH
export LD_LIBRARY_PATH=$PGHOME/lib
export PG_CONFIG=$(which pg_config)

if [ "$PG_PROBACKUP_PTRACK" = "ON" ]; then
    echo "############### Compiling Ptrack:"
    make -C contrib/ptrack install
fi

# Get amcheck if missing
if [ ! -d "contrib/amcheck" ]; then
    echo "############### Getting missing amcheck:"
    git clone https://github.com/petergeoghegan/amcheck.git --depth=1 contrib/amcheck
    make -C contrib/amcheck install
fi

pip3 install -r ../tests/requirements.txt