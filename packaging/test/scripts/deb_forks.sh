#!/usr/bin/env bash

# Copyright Notice:
# © (C) Postgres Professional 2015-2016 http://www.postgrespro.ru/
# Distributed under Apache License 2.0
# Распространяется по лицензии Apache 2.0

set -xe
set -o pipefail

# fix https://github.com/moby/moby/issues/23137
ulimit -n 1024

# TODO: remove after release
exit 0

if [ ${PBK_EDITION} == 'ent' ]; then
    exit 0
fi

if [ ${PBK_EDITION} == 'std' ] && [ ${PG_VERSION} == '9.6' ]; then
    exit 0
fi

# upgrade and utils
# export parameters
export DEBIAN_FRONTEND=noninteractive
echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

#if [ ${CODENAME} == 'jessie' ]; then
#printf "deb http://archive.debian.org/debian/ jessie main\ndeb-src http://archive.debian.org/debian/ jessie main\ndeb http://security.debian.org jessie/updates main\ndeb-src http://security.debian.org jessie/updates main" > /etc/apt/sources.list
#fi

apt-get -qq update
apt-get -qq install -y wget nginx gnupg lsb-release apt-transport-https
#apt-get -qq install -y libterm-readline-gnu-perl dialog gnupg procps

# echo -e 'Package:  *\nPin: origin test.postgrespro.ru\nPin-Priority: 800' >\
#  /etc/apt/preferences.d/pgpro-800

# install nginx
echo "127.0.0.1 test.postgrespro.ru" >> /etc/hosts
cat <<EOF > /etc/nginx/nginx.conf
user www-data;
worker_processes  1;
error_log  /var/log/nginx/error.log;
events {
    worker_connections  1024;
}
http {
    server {
        listen   80 default;
        root /app/www;
    }
}
EOF
nginx -s reload || (pkill -9 nginx || nginx -c /etc/nginx/nginx.conf &)

# install POSTGRESPRO
if [ ${PBK_EDITION} == 'std' ]; then
  sh -c 'echo "deb https://repo.postgrespro.ru/pgpro-${PG_VERSION}/${DISTRIB}/ $(lsb_release -cs) main" > /etc/apt/sources.list.d/pgpro.list'
  wget --quiet -O - https://repo.postgrespro.ru/pgpro-${PG_VERSION}/keys/GPG-KEY-POSTGRESPRO | apt-key add -
  apt-get update -y

  apt-get install -y postgrespro-std-${PG_VERSION}
  BINDIR="/opt/pgpro/std-${PG_VERSION}/bin"
  export LD_LIBRARY_PATH=/opt/pgpro/std-${PG_VERSION}/lib/
fi

# install pg_probackup from current public repo
echo "deb [arch=amd64] http://repo.postgrespro.ru/pg_probackup-forks/deb/ $(lsb_release -cs) main-$(lsb_release -cs)" >\
  /etc/apt/sources.list.d/pg_probackup-old.list
wget -O - http://repo.postgrespro.ru/pg_probackup-forks/keys/GPG-KEY-PG_PROBACKUP | apt-key add - && apt-get update

apt-get install -y pg-probackup-${PBK_EDITION}-${PG_VERSION}
pg_probackup-${PBK_EDITION}-${PG_VERSION} --help
pg_probackup-${PBK_EDITION}-${PG_VERSION} --version


if [ ${PBK_EDITION} == 'std' ]; then
  export PGDATA=/tmp/data
  su postgres -c "${BINDIR}/initdb -k -D ${PGDATA}"
  su postgres -c "pg_probackup-${PBK_EDITION}-${PG_VERSION} init -B /tmp/backup"
  su postgres -c "pg_probackup-${PBK_EDITION}-${PG_VERSION} add-instance --instance=node -B /tmp/backup -D ${PGDATA}"

  echo "wal_level=hot_standby" >> ${PGDATA}/postgresql.auto.conf
  echo "fsync=off" >> ${PGDATA}/postgresql.auto.conf
  echo "archive_mode=on" >> ${PGDATA}/postgresql.auto.conf
  echo "archive_command='pg_probackup-${PBK_EDITION}-${PG_VERSION} archive-push --no-sync -B /tmp/backup compress --instance=node --wal-file-path %p --wal-file-name %f'" >> ${PGDATA}/postgresql.auto.conf

  su postgres -c "${BINDIR}/pg_ctl stop -w -t 60 -D /var/lib/pgpro/std-${PG_VERSION}/data" || echo "it is all good"
  su postgres -c "${BINDIR}/pg_ctl start -D ${PGDATA}"
  sleep 5
  su postgres -c "pg_probackup-${PBK_EDITION}-${PG_VERSION} backup --instance=node -b full -B /tmp/backup -D ${PGDATA} --no-sync"
  su postgres -c "pg_probackup-${PBK_EDITION}-${PG_VERSION} show --instance=node -B /tmp/backup -D ${PGDATA}"
  su postgres -c "pg_probackup-${PBK_EDITION}-${PG_VERSION} show --instance=node -B /tmp/backup -D ${PGDATA} --archive"

  su postgres -c "${BINDIR}/pgbench --no-vacuum -i -s 5"
  su postgres -c "pg_probackup-${PBK_EDITION}-${PG_VERSION} backup --instance=node -b page -B /tmp/backup -D ${PGDATA} --no-sync"
fi

# install new packages
echo "deb [arch=amd64] http://test.postgrespro.ru/pg_probackup-forks/deb/ $(lsb_release -cs) main-$(lsb_release -cs)" >\
	/etc/apt/sources.list.d/pg_probackup-new.list
wget -O - http://test.postgrespro.ru/pg_probackup-forks/keys/GPG-KEY-PG_PROBACKUP | apt-key add -
apt-get update -y

#if [ ${PBK_EDITION} == 'std' ] && [ ${PG_VERSION} == '9.6' ]; then
#  apt-get install -y libpq5 pg-probackup-${PBK_EDITION}-${PG_VERSION}
#else
#  apt-get install -y pg-probackup-${PBK_EDITION}-${PG_VERSION}
#fi

apt-get install -y pg-probackup-${PBK_EDITION}-${PG_VERSION}

# in Ent 11 and 10 because of PQselect vanilla libpq5 is incompatible with Ent pg_probackup
if [ ${PBK_EDITION} == 'ent' ]; then
  if [ ${PG_VERSION} == '11' ] || [ ${PG_VERSION} == '10' ] || [ ${PG_VERSION} == '9.6' ]; then
    exit 0
  fi
fi

pg_probackup-${PBK_EDITION}-${PG_VERSION} --help
pg_probackup-${PBK_EDITION}-${PG_VERSION} --version

if [ ${PBK_EDITION} == 'ent' ]; then
  exit 0
fi

if [ ${PBK_EDITION} == 'std' ] && [ ${PG_VERSION} == '9.6' ]; then
  exit 0
fi


#if [ ${CODENAME} == 'precise' ] && [ ${PG_VERSION} != '10' ] && [ ${PG_VERSION} != '11' ]; then
  su postgres -c "${BINDIR}/pgbench --no-vacuum -t 1000 -c 1"
  su postgres -c "pg_probackup-${PBK_EDITION}-${PG_VERSION} backup --instance=node -b page -B /tmp/backup -D ${PGDATA}"
  su postgres -c "pg_probackup-${PBK_EDITION}-${PG_VERSION} show --instance=node -B /tmp/backup -D ${PGDATA}"

  su postgres -c "${BINDIR}/pg_ctl stop -w -t 60 -D ${PGDATA}"
  rm -rf ${PGDATA}

  su postgres -c "pg_probackup-${PBK_EDITION}-${PG_VERSION} restore --instance=node -B /tmp/backup -D ${PGDATA}"
  su postgres -c "${BINDIR}/pg_ctl start -w -t 60 -D ${PGDATA}"

sleep 5
echo "select count(*) from pgbench_accounts;" | su postgres -c "${BINDIR}/psql" || exit 1

exit 0
