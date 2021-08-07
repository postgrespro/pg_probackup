#!/usr/bin/env bash

# Copyright Notice:
# © (C) Postgres Professional 2015-2021 http://www.postgrespro.ru/
# Distributed under Apache License 2.0
# Распространяется по лицензии Apache 2.0

set -xe
set -o pipefail

ulimit -n 1024

PG_TOG=$(echo $PG_VERSION | sed 's|\.||g')

# upgrade and utils
# export parameters
export DEBIAN_FRONTEND=noninteractive
echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

#if [ ${DISTRIB} == 'ubuntu' ] && [ ${DISTRIB_VERSION} == '14.04' ] && [ ${PG_TOG} == '12' ]; then
#  exit 0
#fi
#
#if [ ${DISTRIB} == 'ubuntu' ] && [ ${DISTRIB_VERSION} == '14.04' ] && [ ${PG_TOG} == '13' ]; then
#  exit 0
#fi

#if [ ${CODENAME} == 'jessie' ]; then
#printf "deb http://archive.debian.org/debian/ jessie main\ndeb-src http://archive.debian.org/debian/ jessie main\ndeb http://security.debian.org jessie/updates main\ndeb-src http://security.debian.org jessie/updates main" > /etc/apt/sources.list
#fi

apt-get -qq update
apt-get -qq install -y wget nginx gnupg lsb-release
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
        root /var/www;
    }
}
EOF
nginx -s reload || (pkill -9 nginx || nginx -c /etc/nginx/nginx.conf &)

# install POSTGRESQL
#if [ ${CODENAME} == 'precise' ] && [ ${PG_VERSION} != '10' ] && [ ${PG_VERSION} != '11' ]; then
  sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
  apt-get update -y
  apt-get install -y postgresql-${PG_VERSION}
#fi

# install pg_probackup from current public repo
echo "deb [arch=amd64] http://repo.postgrespro.ru/pg_probackup/deb/ $(lsb_release -cs) main-$(lsb_release -cs)" >\
  /etc/apt/sources.list.d/pg_probackup-old.list
wget -O - http://repo.postgrespro.ru/pg_probackup/keys/GPG-KEY-PG_PROBACKUP | apt-key add - && apt-get update

apt-get install -y pg-probackup-${PG_VERSION}
pg_probackup-${PG_VERSION} --help
pg_probackup-${PG_VERSION} --version

# Artful do no have PostgreSQL packages at all, Precise do not have PostgreSQL 10
#if [ ${CODENAME} == 'precise' ] && [ ${PG_VERSION} != '10' ] && [ ${PG_VERSION} != '11' ]; then
  export PGDATA=/var/lib/postgresql/${PG_VERSION}/data
  su postgres -c "/usr/lib/postgresql/${PG_VERSION}/bin/initdb -k -D ${PGDATA}"
  su postgres -c "pg_probackup-${PG_VERSION} init -B /tmp/backup"
  su postgres -c "pg_probackup-${PG_VERSION} add-instance --instance=node -B /tmp/backup -D ${PGDATA}"

  echo "wal_level=hot_standby" >> ${PGDATA}/postgresql.auto.conf
  echo "fsync=off" >> ${PGDATA}/postgresql.auto.conf
  echo "archive_mode=on" >> ${PGDATA}/postgresql.auto.conf
  echo "archive_command='pg_probackup-${PG_VERSION} archive-push --no-sync -B /tmp/backup compress --instance=node --wal-file-path %p --wal-file-name %f'" >> ${PGDATA}/postgresql.auto.conf

  su postgres -c "/usr/lib/postgresql/${PG_VERSION}/bin/pg_ctl start -D ${PGDATA}"
  sleep 5
  su postgres -c "pg_probackup-${PG_VERSION} backup --instance=node -b full -B /tmp/backup -D ${PGDATA} --no-sync"
  su postgres -c "pg_probackup-${PG_VERSION} show --instance=node -B /tmp/backup -D ${PGDATA}"
  su postgres -c "pg_probackup-${PG_VERSION} show --instance=node -B /tmp/backup --archive -D ${PGDATA}"

  su postgres -c "/usr/lib/postgresql/${PG_VERSION}/bin/pgbench --no-vacuum -i -s 5"
  su postgres -c "pg_probackup-${PG_VERSION} backup --instance=node -b page -B /tmp/backup -D ${PGDATA} --no-sync"
#fi

# install new packages
echo "deb [arch=amd64] http://test.postgrespro.ru/pg_probackup/deb/ $(lsb_release -cs) main-$(lsb_release -cs)" >\
	/etc/apt/sources.list.d/pg_probackup-new.list
wget -O - http://test.postgrespro.ru/pg_probackup/keys/GPG-KEY-PG_PROBACKUP | apt-key add -
apt-get update
apt-get install -y pg-probackup-${PG_VERSION}
pg_probackup-${PG_VERSION} --help
pg_probackup-${PG_VERSION} --version

#if [ ${CODENAME} == 'precise' ] && [ ${PG_VERSION} != '10' ] && [ ${PG_VERSION} != '11' ]; then
#  echo "wal_level=hot_standby" >> ${PGDATA}/postgresql.auto.conf
#  echo "archive_mode=on" >> ${PGDATA}/postgresql.auto.conf
#  echo "archive_command='${PKG_NAME} archive-push -B /tmp/backup --compress --instance=node --wal-file-path %p --wal-file-name %f'" >> ${PGDATA}/postgresql.auto.conf
#  su postgres -c "/usr/lib/postgresql/${PG_VERSION}/bin/pg_ctl restart -D ${PGDATA}"
#  sleep 5
#  su postgres -c "${PKG_NAME} init -B /tmp/backup"
#  su postgres -c "${PKG_NAME} add-instance --instance=node -B /tmp/backup -D ${PGDATA}"

  su postgres -c "pg_probackup-${PG_VERSION} init -B /tmp/backup"
  su postgres -c "pg_probackup-${PG_VERSION} add-instance --instance=node -B /tmp/backup -D ${PGDATA}"
  su postgres -c "pg_probackup-${PG_VERSION} backup --instance=node --compress -b full -B /tmp/backup -D ${PGDATA} --no-sync"
  su postgres -c "/usr/lib/postgresql/${PG_VERSION}/bin/pgbench --no-vacuum -t 1000 -c 1"
  su postgres -c "pg_probackup-${PG_VERSION} backup --instance=node -b page -B /tmp/backup -D ${PGDATA} --no-sync"
  su postgres -c "pg_probackup-${PG_VERSION} show --instance=node -B /tmp/backup -D ${PGDATA}"

  su postgres -c "/usr/lib/postgresql/${PG_VERSION}/bin/pg_ctl stop -D ${PGDATA}"
  rm -rf ${PGDATA}

  su postgres -c "pg_probackup-${PG_VERSION} restore --instance=node -B /tmp/backup -D ${PGDATA} --no-sync"
  su postgres -c "/usr/lib/postgresql/${PG_VERSION}/bin/pg_ctl start -w -t 60 -D ${PGDATA}"

sleep 5
echo "select count(*) from pgbench_accounts;" | su postgres -c "/usr/lib/postgresql/${PG_VERSION}/bin/psql" || exit 1
#fi

# CHECK SRC package
apt-get install -y dpkg-dev
echo "deb-src [arch=amd64] http://test.postgrespro.ru/pg_probackup/deb/ $(lsb_release -cs) main-$(lsb_release -cs)" >>\
  /etc/apt/sources.list.d/pg_probackup.list

wget -O - http://test.postgrespro.ru/pg_probackup/keys/GPG-KEY-PG_PROBACKUP | apt-key add - && apt-get update

apt-get update -y

cd /mnt
apt-get source pg-probackup-${PG_VERSION}
exit 0

cd pg-probackup-${PG_VERSION}-${PKG_VERSION}
#mk-build-deps --install --remove --tool 'apt-get --no-install-recommends --yes' debian/control
#rm -rf ./*.deb
apt-get install -y debhelper bison flex gettext zlib1g-dev
dpkg-buildpackage -us -uc
