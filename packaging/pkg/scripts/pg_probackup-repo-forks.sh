#!/usr/bin/env bash

# Copyright Notice:
# © (C) Postgres Professional 2015-2016 http://www.postgrespro.ru/
# Distributed under Apache License 2.0
# Распространяется по лицензии Apache 2.0

set -xe
set -o pipefail

# fix https://github.com/moby/moby/issues/23137
ulimit -n 1024

# PACKAGES NEEDED
yum install -y rpm-build

mkdir /root/build
cd /root/build
rpm --rebuilddb && yum clean all

if [ $DISTRIB == 'centos' ]
	then export SHORT_CODENAME='Centos'
elif [ $DISTRIB == 'rhel' ]
	then export SHORT_CODENAME='RedHat'
elif [ $DISTRIB == 'oraclelinux' ]
	then export SHORT_CODENAME='Oracle Linux'
fi

# Copy rpmbuild
cp -rv /app/src/rpm/rpmbuild /root/

# change to build dir
cd /root/rpmbuild/SOURCES
sed -i "s/@DISTRIB@/${DISTRIB}/" pg_probackup-forks.repo
sed -i "s/@SHORT_CODENAME@/${SHORT_CODENAME}/" pg_probackup-forks.repo

cd /root/rpmbuild/SPECS
sed -i "s/@PKG_VERSION@/${PKG_VERSION}/" pg_probackup-repo-forks.spec
sed -i "s/@PKG_RELEASE@/${PKG_RELEASE}/" pg_probackup-repo-forks.spec

# install dependencies
yum-builddep -y pg_probackup-repo-forks.spec

# build repo files
# rpmbuild -bs pg_probackup-repo-pgpro.spec
rpmbuild -ba pg_probackup-repo-forks.spec

# write artefacts to out directory
rm -rf /app/out/*
# cp -arv /root/rpmbuild/{RPMS,SRPMS} /app/out
cp -arv /root/rpmbuild/RPMS /app/out
