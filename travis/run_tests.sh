#!/usr/bin/env bash

#
# Copyright (c) 2019-2020, Postgres Professional
#
set -xe

sudo su -c 'mkdir /run/sshd'
sudo su -c 'apt-get update -y'
sudo su -c 'apt-get install openssh-client openssh-server -y'
sudo su -c '/etc/init.d/ssh start'

ssh-keygen -t rsa -f ~/.ssh/id_rsa -q -N ""
cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys
ssh-keyscan -H localhost >> ~/.ssh/known_hosts

PG_SRC=$PWD/postgres

# # Here PG_VERSION is provided by postgres:X-alpine docker image
# curl "https://ftp.postgresql.org/pub/source/v$PG_VERSION/postgresql-$PG_VERSION.tar.bz2" -o postgresql.tar.bz2
# echo "$PG_SHA256 *postgresql.tar.bz2" | sha256sum -c -

# mkdir $PG_SRC

# tar \
# 	--extract \
# 	--file postgresql.tar.bz2 \
# 	--directory $PG_SRC \
# 	--strip-components 1

# Clone Postgres
echo "############### Getting Postgres sources:"
git clone https://github.com/postgres/postgres.git -b $PG_BRANCH --depth=1

# Clone ptrack
if [ "$APPLY_PTRACK_PATCH" = "on" ]; then
    git clone https://github.com/postgrespro/ptrack.git -b master --depth=1
fi
export PG_PROBACKUP_PTRACK=${APPLY_PTRACK_PATCH}

# Compile and install Postgres
echo "############### Compiling Postgres:"
cd postgres # Go to postgres dir
if [ "$APPLY_PTRACK_PATCH" = "on" ]; then
    git apply -3 ../ptrack/patches/REL_${PG_VERSION}_STABLE-ptrack-core.diff
fi
./configure --prefix=$PGHOME --enable-debug --enable-cassert --enable-depend --enable-tap-tests
make -s -j$(nproc) install
#make -s -j$(nproc) -C 'src/common' install
#make -s -j$(nproc) -C 'src/port' install
#make -s -j$(nproc) -C 'src/interfaces' install
make -s -j$(nproc) -C contrib/ install

if [ "$APPLY_PTRACK_PATCH" = "on" ]; then
    USE_PGXS=1 make -C ../ptrack install
fi

# Override default Postgres instance
export PATH=$PGHOME/bin:$PATH
export LD_LIBRARY_PATH=$PGHOME/lib
export PG_CONFIG=$(which pg_config)

# Get amcheck if missing
if [ ! -d "contrib/amcheck" ]; then
    echo "############### Getting missing amcheck:"
    git clone https://github.com/petergeoghegan/amcheck.git --depth=1 contrib/amcheck
    make USE_PGXS=1 -C contrib/amcheck install
fi

# Get back to testdir
cd ..

# Show pg_config path (just in case)
echo "############### pg_config path:"
which pg_config

# Show pg_config just in case
echo "############### pg_config:"
pg_config

# Build and install pg_probackup (using PG_CPPFLAGS and SHLIB_LINK for gcov)
echo "############### Compiling and installing pg_probackup:"
# make USE_PGXS=1 PG_CPPFLAGS="-coverage" SHLIB_LINK="-coverage" top_srcdir=$CUSTOM_PG_SRC install
make USE_PGXS=1 top_srcdir=$PG_SRC install

# Setup python environment
echo "############### Setting up python env:"
python3 -m virtualenv pyenv
source pyenv/bin/activate
pip3 install testgres

echo "############### Testing:"
if [ "$MODE" = "basic" ]; then
    export PG_PROBACKUP_TEST_BASIC=ON
    python3 -m unittest -v tests
    python3 -m unittest -v tests.init
else
    python3 -m unittest -v tests.$MODE
fi

# Generate *.gcov files
# gcov src/*.c src/*.h

# Send coverage stats to Codecov
# bash <(curl -s https://codecov.io/bash)
