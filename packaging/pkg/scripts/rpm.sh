#!/usr/bin/env bash

# Copyright Notice:
# © (C) Postgres Professional 2015-2016 http://www.postgrespro.ru/
# Distributed under Apache License 2.0
# Распространяется по лицензии Apache 2.0


#yum upgrade -y || echo "some packages in docker fail to install"
#if [ -f /etc/rosa-release ]; then
#	# Avoids old yum bugs on rosa-6
#	yum upgrade -y || echo "some packages in docker fail to install"
#fi

set -xe
set -o pipefail

# fix https://github.com/moby/moby/issues/23137
ulimit -n 1024

# THere is no std/ent packages for PG 9.5
if [[ ${PG_VERSION} == '9.5' ]] && [[ ${PBK_EDITION} != '' ]] ; then
    exit 0
fi

# PACKAGES NEEDED
yum install -y git wget bzip2 rpm-build

mkdir /root/build
cd /root/build
rpm --rebuilddb && yum clean all

# Copy rpmbuild
cp -rv /app/in/specs/rpm/rpmbuild /root/

# download pbk
git clone $PKG_URL pg_probackup-${PKG_VERSION}
cd pg_probackup-${PKG_VERSION}
git checkout ${PKG_HASH}

# move it to source
cd /root/build
if [[ ${PBK_EDITION} == '' ]] ; then
	tar -cjf pg_probackup-${PKG_VERSION}.tar.bz2 pg_probackup-${PKG_VERSION}
	mv pg_probackup-${PKG_VERSION}.tar.bz2 /root/rpmbuild/SOURCES
	rm -rf pg_probackup-${PKG_VERSION}
else
	mv pg_probackup-${PKG_VERSION} /root/rpmbuild/SOURCES
fi

if [[ ${PBK_EDITION} == '' ]] ; then
	cd /root/rpmbuild/SOURCES/
	sed -i "s/@PG_VERSION@/${PKG_VERSION}/" pg_probackup.repo

	# Download PostgreSQL source
	wget http://ftp.postgresql.org/pub/source/v${PG_FULL_VERSION}/postgresql-${PG_FULL_VERSION}.tar.bz2 -O /root/rpmbuild/SOURCES/postgresql-${PG_VERSION}.tar.bz2

	# change to build dir
	cd /root/rpmbuild/SOURCES
	sed -i "s/@DISTRIB@/${DISTRIB}/" pg_probackup.repo
	if [ $DISTRIB == 'centos' ]
		then sed -i "s/@SHORT_CODENAME@/Centos/" pg_probackup.repo
	elif [ $DISTRIB == 'rhel' ]
		then sed -i "s/@SHORT_CODENAME@/RedHat/" pg_probackup.repo
	elif [ $DISTRIB == 'oraclelinux' ]
		then sed -i "s/@SHORT_CODENAME@/Oracle/" pg_probackup.repo
	fi
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

#	tar -cjf postgrespro-${PBK_EDITION}-${PG_FULL_VERSION}.tar.bz2 postgrespro-${PBK_EDITION}-${PG_FULL_VERSION}
fi

cd /root/rpmbuild/SPECS
if [[ ${PBK_EDITION} == '' ]] ; then
	sed -i "s/@PKG_VERSION@/${PKG_VERSION}/" pg_probackup.spec
	sed -i "s/@PKG_RELEASE@/${PKG_RELEASE}/" pg_probackup.spec
	sed -i "s/@PKG_HASH@/${PKG_HASH}/" pg_probackup.spec
	sed -i "s/@PG_VERSION@/${PG_VERSION}/" pg_probackup.spec
	sed -i "s/@PG_FULL_VERSION@/${PG_FULL_VERSION}/" pg_probackup.spec

	sed -i "s/@PG_VERSION@/${PG_VERSION}/" pg_probackup-repo.spec
	sed -i "s/@PKG_VERSION@/${PKG_VERSION}/" pg_probackup-repo.spec
	sed -i "s/@PKG_RELEASE@/${PKG_RELEASE}/" pg_probackup-repo.spec
else
	sed -i "s/@EDITION@/${PBK_EDITION}/" pg_probackup-pgpro.spec
	sed -i "s/@EDITION_FULL@/${PBK_EDITION_FULL}/" pg_probackup-pgpro.spec
	sed -i "s/@PKG_VERSION@/${PKG_VERSION}/" pg_probackup-pgpro.spec
	sed -i "s/@PKG_RELEASE@/${PKG_RELEASE}/" pg_probackup-pgpro.spec
	sed -i "s/@PKG_HASH@/${PKG_HASH}/" pg_probackup-pgpro.spec
	sed -i "s/@PG_VERSION@/${PG_VERSION}/" pg_probackup-pgpro.spec
	sed -i "s/@PG_FULL_VERSION@/${PG_FULL_VERSION}/" pg_probackup-pgpro.spec

	if [ ${PG_VERSION} != '9.6' ]; then
    	sed -i "s|@PREFIX@|/opt/pgpro/${EDITION}-${PG_VERSION}|g" pg_probackup-pgpro.spec
	fi
fi

if [[ ${PBK_EDITION} == '' ]] ; then

	# install dependencies
	yum-builddep -y pg_probackup.spec

	# build pg_probackup
	rpmbuild -bs pg_probackup.spec
	rpmbuild -ba pg_probackup.spec #2>&1 | tee -ai /app/out/build.log

	# build repo files
	rpmbuild -bs pg_probackup-repo.spec
	rpmbuild -ba pg_probackup-repo.spec

	# write artefacts to out directory
	rm -rf /app/out/*
	cp -arv /root/rpmbuild/{RPMS,SRPMS} /app/out
else
	# install dependencies
	yum-builddep -y pg_probackup-pgpro.spec
	# build pg_probackup
	rpmbuild -ba pg_probackup-pgpro.spec #2>&1 | tee -ai /app/out/build.log

	# write artefacts to out directory
	rm -rf /app/out/*
	cp -arv /root/rpmbuild/RPMS /app/out
fi 
