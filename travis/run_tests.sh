#!/usr/bin/env bash

#
# Copyright (c) 2019-2020, Postgres Professional
#


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

# Compile and install Postgres
echo "############### Compiling Postgres:"
cd postgres # Go to postgres dir
./configure --prefix=$PGHOME --enable-debug --enable-cassert --enable-depend --enable-tap-tests
make -s -j$(nproc) install
make -s -j$(nproc) -C contrib/ install

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
make USE_PGXS=1 PG_CFLAGS="-coverage" SHLIB_LINK="-coverage" top_srcdir=$PG_SRC
make USE_PGXS=1 top_srcdir=$PG_SRC install

# Setup python environment
echo "############### Setting up python env:"
python2 -m virtualenv pyenv
source pyenv/bin/activate
pip install testgres

echo "############### Testing:"
if   [ "$MODE" = "basic" ]; then
    export PG_PROBACKUP_TEST_BASIC=ON
    python -m unittest -v tests
    python -m unittest -v tests.init
elif [ "$MODE" = "remote" ]; then

    cat /dev/zero | ssh-keygen -q -N ""
    sudo apt-get install openssh-server -y
    sudo mkdir /run/sshd
    sudo /usr/sbin/sshd -D &
    cat /home/postgres/.ssh/id_rsa.pub > /home/postgres/.ssh/authorized_keys
    ssh-keyscan  localhost >> ~/.ssh/known_hosts

    export PG_PROBACKUP_TEST_BASIC=ON
    export PGPROBACKUP_SSH_REMOTE=ON
    python -m unittest -v tests
else
    python -m unittest -v tests.$MODE
fi

# Generate *.gcov files
gcov src/*.c src/*.h

# Send coverage stats to Codecov
bash <(curl -s https://codecov.io/bash) -t fb9cf24e-be3d-48ef-9e98-2861a1406209
