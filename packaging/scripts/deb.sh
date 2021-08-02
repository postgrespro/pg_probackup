#!/usr/bin/env bash

# Copyright Notice:
# © (C) Postgres Professional 2015-2016 http://www.postgrespro.ru/
# Distributed under Apache License 2.0
# Распространяется по лицензии Apache 2.0

set -xe
set -o pipefail

# fix https://github.com/moby/moby/issues/23137
ulimit -n 1024

# THere is no std/ent packages for PG 9.5
#echo ${PG_MAJOUR_VERSION}
#echo ${PBK_EDITION}
if [[ ${PG_VERSION} == '9.5' ]] && [[ ${PBK_EDITION} != '' ]] ; then
    exit 0
fi

# PACKAGES NEEDED
apt-get update -y && apt-get install -y git wget bzip2 devscripts equivs

# Prepare
export DEBIAN_FRONTEND=noninteractive
echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

if [ ${CODENAME} == 'jessie' ]; then
printf "deb http://archive.debian.org/debian/ jessie main\ndeb-src http://archive.debian.org/debian/ jessie main\ndeb http://security.debian.org jessie/updates main\ndeb-src http://security.debian.org jessie/updates main" > /etc/apt/sources.list
fi

apt-get -qq update -y

# download PKG_URL if PKG_HASH is omitted
mkdir /root/build
cd /root/build

# clone pbk repo
git clone $PKG_URL ${PKG_NAME}_${PKG_VERSION}
cd ${PKG_NAME}_${PKG_VERSION}
git fetch -a
git checkout ${PKG_HASH}
cd ..

PG_TOC=$(echo ${PG_VERSION} | sed 's|\.||g')
# Download PostgreSQL source
if [[ ${PBK_EDITION} == '' ]] ; then
    wget -q http://ftp.postgresql.org/pub/source/v${PG_FULL_VERSION}/postgresql-${PG_FULL_VERSION}.tar.bz2
fi

cd /root/build/${PKG_NAME}_${PKG_VERSION}
cp -av /app/in/packaging/specs/deb/pg_probackup/debian ./
if [[ ${PBK_EDITION} == '' ]] ; then
    sed -i "s/@PKG_NAME@/${PKG_NAME}/g" debian/changelog
    sed -i "s/@PKG_VERSION@/${PKG_VERSION}/g" debian/changelog
    sed -i "s/@PKG_RELEASE@/${PKG_RELEASE}/g" debian/changelog
    sed -i "s/@PKG_HASH@/${PKG_HASH}/g" debian/changelog
    sed -i "s/@CODENAME@/${CODENAME}/g" debian/changelog

    sed -i "s/@PKG_NAME@/${PKG_NAME}/g" debian/control
    sed -i "s/@PG_VERSION@/${PG_VERSION}/g" debian/control

    sed -i "s/@PG_VERSION@/${PG_VERSION}/" debian/pg_probackup.install
    mv debian/pg_probackup.install debian/${PKG_NAME}.install

    sed -i "s/@PKG_NAME@/${PKG_NAME}/g" debian/rules
    sed -i "s/@PG_TOC@/${PG_TOC}/g" debian/rules
    sed -i "s/@PG_VERSION@/${PG_VERSION}/g" debian/rules
    sed -i "s/@PG_FULL_VERSION@/${PG_FULL_VERSION}/g" debian/rules
    sed -i "s|@PREFIX@|/stump|g" debian/rules
else
    sed -i "s/@PKG_NAME@/pg-probackup-${PBK_EDITION}-${PG_VERSION}/g" debian/changelog
    sed -i "s/@PKG_VERSION@/${PKG_VERSION}/g" debian/changelog
    sed -i "s/@PKG_RELEASE@/${PKG_RELEASE}/g" debian/changelog
    sed -i "s/@PKG_HASH@/${PKG_HASH}/g" debian/changelog
    sed -i "s/@CODENAME@/${CODENAME}/g" debian/changelog

    sed -i "s/@PKG_NAME@/pg-probackup-${PBK_EDITION}-${PG_VERSION}/g" debian/control
    sed -i "s/pg-probackup-@PG_VERSION@/pg-probackup-${PBK_EDITION}-${PG_VERSION}/g" debian/control
    sed -i "s/@PG_VERSION@/${PG_VERSION}/g" debian/control
    sed -i "s/PostgreSQL/PostgresPro ${PBK_EDITION_FULL}/g" debian/control

    sed -i "s/pg_probackup-@PG_VERSION@/pg_probackup-${PBK_EDITION}-${PG_VERSION}/" debian/pg_probackup.install
    mv debian/pg_probackup.install debian/pg-probackup-${PBK_EDITION}-${PG_VERSION}.install

    sed -i "s/@PKG_NAME@/pg-probackup-${PBK_EDITION}-${PG_VERSION}/g" debian/rules
    sed -i "s/@PG_TOC@/${PG_TOC}/g" debian/rules
    sed -i "s/pg_probackup-@PG_VERSION@/pg_probackup-${PBK_EDITION}-${PG_VERSION}/g" debian/rules
    sed -i "s/postgresql-@PG_FULL_VERSION@/postgrespro-${PBK_EDITION}-${PG_FULL_VERSION}/g" debian/rules

    if [ ${PG_VERSION} == '9.6' ]; then
        sed -i "s|@PREFIX@|/stump|g" debian/rules
    else
        sed -i "s|@PREFIX@|/opt/pgpro/${PBK_EDITION}-${PG_VERSION}|g" debian/rules
    fi
fi

# Build dependencies
mk-build-deps --install --remove --tool 'apt-get --no-install-recommends --yes' debian/control
rm -rf ./*.deb

# Pack source to orig.tar.gz
mkdir -p /root/build/dsc
if [[ ${PBK_EDITION} == '' ]] ; then
    mv /root/build/postgresql-${PG_FULL_VERSION}.tar.bz2 \
        /root/build/dsc/${PKG_NAME}_${PKG_VERSION}.orig-postgresql${PG_TOC}.tar.bz2

    cd /root/build/${PKG_NAME}_${PKG_VERSION}
    tar -xf /root/build/dsc/${PKG_NAME}_${PKG_VERSION}.orig-postgresql${PG_TOC}.tar.bz2
    cd /root/build

    tar -czf ${PKG_NAME}_${PKG_VERSION}.orig.tar.gz \
        ${PKG_NAME}_${PKG_VERSION}

    mv /root/build/${PKG_NAME}_${PKG_VERSION}.orig.tar.gz /root/build/dsc

    cd /root/build/${PKG_NAME}_${PKG_VERSION}
    tar -xf /root/build/dsc/${PKG_NAME}_${PKG_VERSION}.orig-postgresql${PG_TOC}.tar.bz2
else
    tar -xf /app/in/packaging/tarballs/pgpro.tar.bz2 -C /root/build/dsc/
    cd /root/build/dsc/pgpro

    PGPRO_TOC=$(echo ${PG_FULL_VERSION} | sed 's|\.|_|g')
    if [[ ${PBK_EDITION} == 'std' ]] ; then
        git checkout "PGPRO${PGPRO_TOC}_1"
    else
        git checkout "PGPROEE${PGPRO_TOC}_1"
    fi

    mv /root/build/dsc/pgpro /root/build/${PKG_NAME}_${PKG_VERSION}/postgrespro-${PBK_EDITION}-${PG_FULL_VERSION}
fi

# BUILD: SOURCE PKG
if [[ ${PBK_EDITION} == '' ]] ; then
    cd /root/build/dsc
    dpkg-source -b /root/build/${PKG_NAME}_${PKG_VERSION}
fi

# BUILD: DEB PKG
cd /root/build/${PKG_NAME}_${PKG_VERSION}
dpkg-buildpackage -b #&> /app/out/build.log

# COPY ARTEFACTS
rm -rf /app/out/*
cd /root/build
cp -v *.deb /app/out
cp -v *.changes /app/out

if [[ ${PBK_EDITION} == '' ]] ; then
    cp -arv dsc /app/out
fi
