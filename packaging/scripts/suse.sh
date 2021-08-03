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
zypper clean

# PACKAGES NEEDED
zypper install -y git wget bzip2 rpm-build

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
tar -cjf pg_probackup-${PKG_VERSION}.tar.bz2 pg_probackup-${PKG_VERSION}
mv pg_probackup-${PKG_VERSION}.tar.bz2 /root/rpmbuild/SOURCES
rm -rf pg_probackup-${PKG_VERSION}

# Download PostgreSQL source
wget -q http://ftp.postgresql.org/pub/source/v${PG_FULL_VERSION}/postgresql-${PG_FULL_VERSION}.tar.bz2 -O /root/rpmbuild/SOURCES/postgresql-${PG_VERSION}.tar.bz2

#mv pg_probackup-${PKG_VERSION}.tar.bz2 /usr/src/packages/SOURCES/
#mv postgresql-${PG_VERSION}.tar.bz2 /usr/src/packages/SOURCES/
#cp -av /root/rpmbuild/SOURCES/GPG-KEY-PG_PROBACKUP /usr/src/packages/SOURCES/GPG-KEY-PG_PROBACKUP
#cp -av /root/rpmbuild/SOURCES/pg_probackup.repo /usr/src/packages/SOURCES/pg_probackup.repo

rm -rf /usr/src/packages
ln -s /root/rpmbuild /usr/src/packages

cd /root/rpmbuild/SOURCES
sed -i "s/@PG_VERSION@/${PKG_VERSION}/" pg_probackup.repo


# change to build dir
cd /root/rpmbuild/SOURCES
sed -i "s/@DISTRIB@/${DISTRIB}/" pg_probackup.repo
if [ $DISTRIB == 'centos' ]
	then sed -i "s/@SHORT_CODENAME@/Centos/" pg_probackup.repo
elif [ $DISTRIB == 'rhel' ]
	then sed -i "s/@SHORT_CODENAME@/RedHat/" pg_probackup.repo
elif [ $DISTRIB == 'oraclelinux' ]
	then sed -i "s/@SHORT_CODENAME@/Oracle/" pg_probackup.repo
elif [ $DISTRIB == 'suse' ]
	then sed -i "s/@SHORT_CODENAME@/SUSE/" pg_probackup.repo
fi

cd /root/rpmbuild/SPECS
sed -i "s/@PKG_VERSION@/${PKG_VERSION}/" pg_probackup.spec
sed -i "s/@PKG_RELEASE@/${PKG_RELEASE}/" pg_probackup.spec
sed -i "s/@PKG_HASH@/${PKG_HASH}/" pg_probackup.spec
sed -i "s/@PG_VERSION@/${PG_VERSION}/" pg_probackup.spec
sed -i "s/@PG_FULL_VERSION@/${PG_FULL_VERSION}/" pg_probackup.spec

sed -i "s/@PG_VERSION@/${PG_VERSION}/" pg_probackup-repo.spec
sed -i "s/@PKG_VERSION@/${PKG_VERSION}/" pg_probackup-repo.spec
sed -i "s/@PKG_RELEASE@/${PKG_RELEASE}/" pg_probackup-repo.spec

# install dependencies
zypper -n install \
    $(rpmspec --parse pg_probackup.spec | grep BuildRequires | cut -d':' -f2 | xargs)

# build pg_probackup
rpmbuild -bs pg_probackup.spec
rpmbuild -ba pg_probackup.spec #2>&1 | tee -ai /app/out/build.log

# build repo files, TODO: move to separate repo
rpmbuild -ba pg_probackup-repo.spec

# write artefacts to out directory
rm -rf /app/out/*
#sleep 100500
cp -arv /root/rpmbuild/{RPMS,SRPMS} /app/out
#mkdir -p /app/out/RPMS/x86_64
#mkdir -p /app/out/RPMS/noarch
#cp -arv /usr/src/packages/RPMS/x86_64/*rpm /app/out/RPMS/x86_64/
#cp -arv /usr/src/packages/RPMS/noarch/*rpm /app/out/RPMS/noarch/
