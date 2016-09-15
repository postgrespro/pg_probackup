#!/bin/sh -ex

# vars
export PGVERSION=9.5.4
export PATH=$PATH:/usr/pgsql-9.5/bin
export PGUSER=pgbench
export PGDATABASE=pgbench
export PGDATA=/var/lib/pgsql/9.5/data
export BACKUP_PATH=/backups
export ARCLOG_PATH=$BACKUP_PATH/backup/pg_xlog
export PGDATA2=/var/lib/pgsql/9.5/data2
export PGBENCH_SCALE=100
export PGBENCH_TIME=60

# prepare directory
cp -a /tests /build
pushd /build

# download postgresql
yum install -y wget
wget -k https://ftp.postgresql.org/pub/source/v$PGVERSION/postgresql-$PGVERSION.tar.gz -O postgresql.tar.gz
tar xf postgresql.tar.gz

# install pg_arman
yum install -y https://download.postgresql.org/pub/repos/yum/9.5/redhat/rhel-7-x86_64/pgdg-centos95-9.5-2.noarch.rpm
yum install -y postgresql95-devel make gcc readline-devel openssl-devel pam-devel libxml2-devel libxslt-devel
make top_srcdir=postgresql-$PGVERSION
make install top_srcdir=postgresql-$PGVERSION

# initalize cluster and database
yum install -y postgresql95-server
su postgres -c "/usr/pgsql-9.5/bin/initdb -D $PGDATA -k"
cat <<EOF > $PGDATA/pg_hba.conf
local   all             all                                trust
host    all             all        127.0.0.1/32            trust
local   replication     pgbench                            trust
host    replication     pgbench    127.0.0.1/32            trust
EOF
cat <<EOF > $PGDATA/postgresql.auto.conf
max_wal_senders = 2
wal_level = logical
wal_log_hints = on
EOF
su postgres -c "/usr/pgsql-9.5/bin/pg_ctl start -w -D $PGDATA"
su postgres -c "createdb -U postgres $PGUSER"
su postgres -c "createuser -U postgres -a -d -E $PGUSER"
pgbench -i -s $PGBENCH_SCALE

# Count current
COUNT=$(psql -Atc "select count(*) from pgbench_accounts")
pgbench -s $PGBENCH_SCALE -T $PGBENCH_TIME -j 2 -c 10 &

# create backup
pg_arman init
pg_arman backup -b full --disable-ptrack-clear --stream -v
pg_arman show
sleep $PGBENCH_TIME

# restore from backup
chown -R postgres:postgres $BACKUP_PATH
su postgres -c "pg_arman restore -D $PGDATA2"

# start backup server
su postgres -c "/usr/pgsql-9.5/bin/pg_ctl stop -w -D $PGDATA"
su postgres -c "/usr/pgsql-9.5/bin/pg_ctl start -w -D $PGDATA2"
( psql -Atc "select count(*) from pgbench_accounts" | grep $COUNT ) || (cat $PGDATA2/pg_log/*.log ; exit 1)
