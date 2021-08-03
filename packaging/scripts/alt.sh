#!/usr/bin/env bash

# Copyright Notice:
# © (C) Postgres Professional 2015-2016 http://www.postgrespro.ru/
# Distributed under Apache License 2.0
# Распространяется по лицензии Apache 2.0

set -xe
set -o pipefail

# THere is no std/ent packages for PG 9.5
if [[ ${PG_VERSION} == '9.5' ]] && [[ ${PBK_EDITION} != '' ]] ; then
    exit 0
fi

# fix https://github.com/moby/moby/issues/23137
ulimit -n 1024
apt-get update -y

mkdir /root/build
cd /root/build

# Copy rpmbuild
cp -rv /app/in/specs/rpm/rpmbuild /root/

# download pbk
git clone $PKG_URL pg_probackup-${PKG_VERSION}
cd pg_probackup-${PKG_VERSION}
git checkout ${PKG_HASH}
cd ..

# tarball it
if [[ ${PBK_EDITION} == '' ]] ; then
	tar -cjf pg_probackup-${PKG_VERSION}.tar.bz2 pg_probackup-${PKG_VERSION}
	mv pg_probackup-${PKG_VERSION}.tar.bz2 /root/rpmbuild/SOURCES
	rm -rf pg_probackup-${PKG_VERSION}
else
	mv pg_probackup-${PKG_VERSION} /root/rpmbuild/SOURCES
fi


if [[ ${PBK_EDITION} == '' ]] ; then
	# Download PostgreSQL source
	wget -q http://ftp.postgresql.org/pub/source/v${PG_FULL_VERSION}/postgresql-${PG_FULL_VERSION}.tar.bz2 -O postgresql-${PG_VERSION}.tar.bz2
	mv postgresql-${PG_VERSION}.tar.bz2 /root/rpmbuild/SOURCES/

else
	tar -xf /app/in/tarballs/pgpro.tar.bz2 -C /root/rpmbuild/SOURCES/
	cd /root/rpmbuild/SOURCES/pgpro

	PGPRO_TOC=$(echo ${PG_FULL_VERSION} | sed 's|\.|_|g')
    if [[ ${PBK_EDITION} == 'std' ]] ; then
        git checkout "PGPRO${PGPRO_TOC}_1"
    else
        git checkout "PGPROEE${PGPRO_TOC}_1"
    fi
	rm -rf .git

	cd /root/rpmbuild/SOURCES/
	mv pgpro postgrespro-${PBK_EDITION}-${PG_FULL_VERSION}
	chown -R root:root postgrespro-${PBK_EDITION}-${PG_FULL_VERSION}
fi


cd /root/rpmbuild/SOURCES
sed -i "s/@PG_VERSION@/${PKG_VERSION}/" pg_probackup.repo

# build postgresql
echo '%_allow_root_build      yes' > /root/.rpmmacros
echo '%_topdir %{getenv:HOME}/rpmbuild' >> /root/.rpmmacros

cd /root/rpmbuild/SPECS
if [[ ${PBK_EDITION} == '' ]] ; then
	sed -i "s/@PKG_VERSION@/${PKG_VERSION}/" pg_probackup.alt.spec
	sed -i "s/@PKG_RELEASE@/${PKG_RELEASE}/" pg_probackup.alt.spec
	sed -i "s/@PKG_HASH@/${PKG_HASH}/" pg_probackup.alt.spec
	sed -i "s/@PG_VERSION@/${PG_VERSION}/" pg_probackup.alt.spec
	sed -i "s/@PG_FULL_VERSION@/${PG_FULL_VERSION}/" pg_probackup.alt.spec
else
	sed -i "s/@EDITION@/${PBK_EDITION}/" pg_probackup.alt.forks.spec
	sed -i "s/@EDITION_FULL@/${PBK_EDITION_FULL}/" pg_probackup.alt.forks.spec
	sed -i "s/@PKG_VERSION@/${PKG_VERSION}/" pg_probackup.alt.forks.spec
	sed -i "s/@PKG_RELEASE@/${PKG_RELEASE}/" pg_probackup.alt.forks.spec
	sed -i "s/@PKG_HASH@/${PKG_HASH}/" pg_probackup.alt.forks.spec
	sed -i "s/@PG_VERSION@/${PG_VERSION}/" pg_probackup.alt.forks.spec
	sed -i "s/@PG_FULL_VERSION@/${PG_FULL_VERSION}/" pg_probackup.alt.forks.spec

	if [ ${PG_VERSION} != '9.6' ]; then
	    sed -i "s|@PREFIX@|/opt/pgpro/${EDITION}-${PG_VERSION}|g" pg_probackup.alt.forks.spec
	fi
fi

# ALT Linux suck as detecting dependecies, so the manual hint is required
if [ ${DISTRIB_VERSION} == '7' ]; then
	apt-get install libpq5.10

elif [ ${DISTRIB_VERSION} == '8' ]; then
	apt-get install libpq5.12

else
	apt-get install libpq5
fi

# install dependencies
#stolen from postgrespro
apt-get install -y flex libldap-devel libpam-devel libreadline-devel libssl-devel

if [[ ${PBK_EDITION} == '' ]] ; then

	# build pg_probackup
	rpmbuild -bs pg_probackup.alt.spec
	rpmbuild -ba pg_probackup.alt.spec #2>&1 | tee -ai /app/out/build.log

	# write artefacts to out directory
	rm -rf /app/out/*
	cp -arv /root/rpmbuild/{RPMS,SRPMS} /app/out
else
	rpmbuild -ba pg_probackup.alt.forks.spec #2>&1 | tee -ai /app/out/build.log
	# write artefacts to out directory
	rm -rf /app/out/*
	# cp -arv /root/rpmbuild/{RPMS,SRPMS} /app/out
	cp -arv /root/rpmbuild/RPMS /app/out
fi
