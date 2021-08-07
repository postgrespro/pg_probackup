#!/usr/bin/env bash

# Copyright Notice:
# © (C) Postgres Professional 2015-2016 http://www.postgrespro.ru/
# Distributed under Apache License 2.0
# Распространяется по лицензии Apache 2.0

set -xe
set -o pipefail

# fix https://github.com/moby/moby/issues/23137
ulimit -n 1024

PG_TOG=$(echo $PG_VERSION | sed 's|\.||g')

if [ ${EDITION} == 'ent' ]; then
    exit 0
fi

# yum upgrade -y || echo 'some packages in docker failed to upgrade'
# yum install -y sudo

if [ ${DISTRIB} == 'rhel' ] && [ ${DISTRIB_VERSION} == '6' ]; then
    exit 0;
elif [ ${DISTRIB} == 'oraclelinux' ] && [ ${DISTRIB_VERSION} == '6' ]; then
    exit 0;
elif [ ${DISTRIB} == 'oraclelinux' ] && [ ${DISTRIB_VERSION} == '8' ]; then
    yum install -y nginx
elif [ ${DISTRIB_VERSION} == '7' ]; then
    yum install -y https://nginx.org/packages/rhel/7/x86_64/RPMS/nginx-1.8.1-1.el7.ngx.x86_64.rpm
elif [ ${DISTRIB} == 'oraclelinux' ] && [ ${DISTRIB_VERSION} == '6' ]; then
    yum install -y https://nginx.org/packages/rhel/6/x86_64/RPMS/nginx-1.8.1-1.el6.ngx.x86_64.rpm
else
    yum install epel-release -y
    yum install -y nginx
fi

if ! getent group nginx > /dev/null 2>&1 ; then
    addgroup --system --quiet nginx
fi
if ! getent passwd nginx > /dev/null 2>&1 ; then
    adduser --quiet \
        --system --disabled-login --ingroup nginx \
        --home /var/run/nginx/ --no-create-home \
        nginx
fi

cat <<EOF > /etc/nginx/nginx.conf
user nginx;
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

# if [ ${DISTRIB} == 'centos' ]; then

# install old packages
yum install -y http://repo.postgrespro.ru/pg_probackup-forks/keys/pg_probackup-repo-forks-${DISTRIB}.noarch.rpm
sed -i "s/https/http/g" /etc/yum.repos.d/pg_probackup-forks.repo

yum install -y ${PKG_NAME}
${PKG_NAME} --help
${PKG_NAME} --version

if [ $EDITION == 'std' ] ; then

    # install POSTGRESQL
    # rpm -ivh https://download.postgresql.org/pub/repos/yum/reporpms/EL-${DISTRIB_VERSION}-x86_64/pgdg-redhat-repo-latest.noarch.rpm
    if [[ ${PG_VERSION} == '11' ]] || [[ ${PG_VERSION} == '12' ]]; then
        rpm -ivh https://repo.postgrespro.ru/pgpro-${PG_VERSION}/keys/postgrespro-std-${PG_VERSION}.${DISTRIB}.yum-${PG_VERSION}-0.3.noarch.rpm
    else
        rpm -ivh https://repo.postgrespro.ru/pgpro-${PG_VERSION}/keys/postgrespro-std-${PG_VERSION}.${DISTRIB}.yum-${PG_VERSION}-0.3.noarch.rpm
    fi

    if [[ ${PG_VERSION} == '9.6' ]]; then
        yum install -y postgrespro${PG_TOG}-server.x86_64
        BINDIR="/usr/pgpro-${PG_VERSION}/bin"
    else
        yum install -y postgrespro-std-${PG_TOG}-server.x86_64
        BINDIR="/opt/pgpro/std-${PG_VERSION}/bin"
        export LD_LIBRARY_PATH=/opt/pgpro/std-${PG_VERSION}/lib/
    fi

    export PGDATA=/tmp/data

    su postgres -c "${BINDIR}/initdb -k -D ${PGDATA}"
    echo "wal_level=hot_standby" >> ${PGDATA}/postgresql.auto.conf
    echo "archive_mode=on" >> ${PGDATA}/postgresql.auto.conf
    echo "fsync=off" >> ${PGDATA}/postgresql.auto.conf
    echo "archive_command='${PKG_NAME} archive-push -B /tmp/backup --instance=node --wal-file-path %p --wal-file-name %f'" >> ${PGDATA}/postgresql.auto.conf
    su postgres -c "${BINDIR}/pg_ctl start -D ${PGDATA}"
    sleep 5

    su postgres -c "${PKG_NAME} init -B /tmp/backup"
    su postgres -c "${PKG_NAME} add-instance --instance=node -B /tmp/backup -D ${PGDATA}"
    su postgres -c "${PKG_NAME} backup --instance=node --compress -b full -B /tmp/backup -D ${PGDATA}"
    su postgres -c "${PKG_NAME} show --instance=node -B /tmp/backup -D ${PGDATA} --archive"

    su postgres -c "${BINDIR}/pgbench --no-vacuum -i -s 5"
    su postgres -c "${PKG_NAME} backup --instance=node -b page -B /tmp/backup -D ${PGDATA}"
fi

# install new packages
echo "127.0.0.1 repo.postgrespro.ru" >> /etc/hosts

# yum remove -y pg_probackup-repo
#yum install -y http://repo.postgrespro.ru/pg_probackup-forks/keys/pg_probackup-repo-forks-${DISTRIB}.noarch.rpm
#yum clean all -y

sed -i "s/https/http/g" /etc/yum.repos.d/pg_probackup-forks.repo

# yum update -y ${PKG_NAME}
yum install -y ${PKG_NAME}
sleep 1
${PKG_NAME} --help
${PKG_NAME} --version

if [ $EDITION == 'ent' ]; then
    exit 0
fi

# su postgres -c "${BINDIR}/pgbench --no-vacuum -t 1000 -c 1"
su postgres -c "${BINDIR}/pgbench --no-vacuum -i -s 5"
su postgres -c "${PKG_NAME} backup --instance=node -b full -B /tmp/backup -D ${PGDATA}"
su postgres -c "${PKG_NAME} show --instance=node -B /tmp/backup -D ${PGDATA}"

su postgres -c "${BINDIR}/pg_ctl stop -D ${PGDATA}"
rm -rf ${PGDATA}

su postgres -c "${PKG_NAME} restore --instance=node -B /tmp/backup -D ${PGDATA}"
su postgres -c "${BINDIR}/pg_ctl start -w -D ${PGDATA}"

sleep 5

echo "select count(*) from pgbench_accounts;" | su postgres -c "${BINDIR}/psql" || exit 1

#else
#    echo "127.0.0.1 repo.postgrespro.ru" >> /etc/hosts
#    rpm -ivh http://repo.postgrespro.ru/pg_probackup/keys/pg_probackup-repo-${DISTRIB}.noarch.rpm
#    yum install -y ${PKG_NAME}
#    ${PKG_NAME} --help
#    su postgres -c "/usr/pgsql-${PG_VERSION}/bin/initdb -k -D ${PGDATA}"
#    su postgres -c "${PKG_NAME} init -B /tmp/backup"
#    su postgres -c "${PKG_NAME} add-instance --instance=node -B /tmp/backup -D ${PGDATA}"
#    echo "wal_level=hot_standby" >> ${PGDATA}/postgresql.auto.conf
#    echo "archive_mode=on" >> ${PGDATA}/postgresql.auto.conf
#    echo "archive_command='${PKG_NAME} archive-push -B /tmp/backup --instance=node --wal-file-path %p --wal-file-name %f'" >> ${PGDATA}/postgresql.auto.conf
#    su postgres -c "/usr/pgsql-${PG_VERSION}/bin/pg_ctl start -D ${PGDATA}"
#    sleep 5
#    su postgres -c "${PKG_NAME} backup --instance=node --compress -b full -B /tmp/backup -D ${PGDATA}"
#    su postgres -c "/usr/pgsql-${PG_VERSION}/bin/pgbench --no-vacuum -i -s 10"
#    su postgres -c "${PKG_NAME} backup --instance=node -b page -B /tmp/backup -D ${PGDATA}"
#    su postgres -c "/usr/pgsql-${PG_VERSION}/bin/pgbench --no-vacuum -t 1000 -c 1"
#    su postgres -c "${PKG_NAME} backup --instance=node -b page -B /tmp/backup -D ${PGDATA}"
#    su postgres -c "${PKG_NAME} show --instance=node -B /tmp/backup -D ${PGDATA}"
#    su postgres -c "/usr/pgsql-${PG_VERSION}/bin/pg_ctl stop -D ${PGDATA}"
#    rm -rf ${PGDATA}
#    su postgres -c "${PKG_NAME} restore --instance=node -B /tmp/backup -D ${PGDATA}"
#    su postgres -c "/usr/pgsql-${PG_VERSION}/bin/pg_ctl start -D ${PGDATA}"
#    sleep 10
#    echo "select count(*) from pgbench_accounts;" | su postgres -c "/usr/pgsql-${PG_VERSION}/bin/psql" || exit 1
#fi

exit 0
