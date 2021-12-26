#!/usr/bin/env bash

# Copyright Notice:
# © (C) Postgres Professional 2015-2016 http://www.postgrespro.ru/
# Distributed under Apache License 2.0
# Распространяется по лицензии Apache 2.0

set -ex
set -o errexit
set -o pipefail

# fix https://github.com/moby/moby/issues/23137
ulimit -n 1024

export INPUT_DIR=/app/in #dir with builded rpm
export OUT_DIR=/app/www/${PBK_PKG_REPO}
export KEYS_DIR=$OUT_DIR/keys

# deploy keys
mkdir -p "$KEYS_DIR"
rsync /app/repo/$PBK_PKG_REPO/gnupg/key.public $KEYS_DIR/GPG-KEY-PG_PROBACKUP
chmod 755 $KEYS_DIR
echo -e 'User-agent: *\nDisallow: /' > $OUT_DIR/robots.txt

cd $INPUT_DIR

cp -arv /app/repo/rpm-conf/rpmmacros /root/.rpmmacros
cp -arv /app/repo/$PBK_PKG_REPO/gnupg /root/.gnupg
chmod -R 0600 /root/.gnupg
chown -R root:root /root/.gnupg

for pkg in $(ls ${INPUT_DIR}); do
	for pkg_full_version in $(ls ${INPUT_DIR}/$pkg); do
		if [[ ${PBK_EDITION} == '' ]] ; then
			cp $INPUT_DIR/$pkg/$pkg_full_version/RPMS/noarch/pg_probackup-repo-*.noarch.rpm \
				$KEYS_DIR/pg_probackup-repo-$DISTRIB.noarch.rpm
		else
			cp $INPUT_DIR/$pkg/$pkg_full_version/RPMS/noarch/pg_probackup-repo-*.noarch.rpm \
				$KEYS_DIR/pg_probackup-repo-forks-$DISTRIB.noarch.rpm
		fi

		[ ! -z "$CODENAME" ] && export DISTRIB_VERSION=$CODENAME
		RPM_DIR=$OUT_DIR/rpm/$pkg_full_version/${DISTRIB}-${DISTRIB_VERSION}-x86_64
		mkdir -p "$RPM_DIR"
		cp -arv $INPUT_DIR/$pkg/$pkg_full_version/RPMS/x86_64/* $RPM_DIR/
		for f in $(ls $RPM_DIR/*.rpm); do rpm --addsign $f || exit 1; done
		createrepo $RPM_DIR/

		if [[ ${PBK_EDITION} == '' ]] ; then
			SRPM_DIR=$OUT_DIR/srpm/$pkg_full_version/${DISTRIB}-${DISTRIB_VERSION}-x86_64
			mkdir -p "$SRPM_DIR"
			cp -arv $INPUT_DIR/$pkg/$pkg_full_version/SRPMS/* $SRPM_DIR/
			for f in $(ls $SRPM_DIR/*.rpm); do rpm --addsign $f || exit 1; done
			createrepo $SRPM_DIR/
		fi

	done
done
