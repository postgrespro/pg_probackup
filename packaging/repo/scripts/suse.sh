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

zypper install -y createrepo
rm -rf /root/.gnupg

cd $INPUT_DIR

mkdir -p $KEYS_DIR
chmod 755 $KEYS_DIR
rsync /app/repo/$PBK_PKG_REPO/gnupg/key.public $KEYS_DIR/GPG-KEY-PG_PROBACKUP

echo -e 'User-agent: *\nDisallow: /' > $OUT_DIR/robots.txt

cp -arv /app/repo/$PBK_PKG_REPO/rpmmacros /root/.rpmmacros
cp -arv /app/repo/$PBK_PKG_REPO/gnupg /root/.gnupg
chmod -R 0600 /root/.gnupg

for pkg in $(ls); do
	for pkg_full_version in $(ls ./$pkg); do

		cp $INPUT_DIR/$pkg/$pkg_full_version/RPMS/noarch/pg_probackup-repo-*.noarch.rpm \
			$KEYS_DIR/pg_probackup-repo-$DISTRIB.noarch.rpm
		[ ! -z "$CODENAME" ] && export DISTRIB_VERSION=$CODENAME
		RPM_DIR=$OUT_DIR/rpm/$pkg_full_version/${DISTRIB}-${DISTRIB_VERSION}-x86_64
		SRPM_DIR=$OUT_DIR/srpm/$pkg_full_version/${DISTRIB}-${DISTRIB_VERSION}-x86_64

		# rm -rf "$RPM_DIR" && mkdir -p "$RPM_DIR"
		# rm -rf "$SRPM_DIR" && mkdir -p "$SRPM_DIR"
		mkdir -p "$RPM_DIR"
		mkdir -p "$SRPM_DIR"

		cp -arv $INPUT_DIR/$pkg/$pkg_full_version/RPMS/x86_64/* $RPM_DIR/
		cp -arv $INPUT_DIR/$pkg/$pkg_full_version/SRPMS/* $SRPM_DIR/

		for f in $(ls $RPM_DIR/*.rpm); do rpm --addsign $f || exit 1; done
		for f in $(ls $SRPM_DIR/*.rpm); do rpm --addsign $f || exit 1; done

		createrepo $RPM_DIR/
		createrepo $SRPM_DIR/

		# rpm --addsign $RPM_DIR/repodata/repomd.xml
		# rpm --addsign $SRPM_DIR/repodata/repomd.xml
	done
done

gpg -a --detach-sign $RPM_DIR/repodata/repomd.xml
gpg -a --detach-sign $SRPM_DIR/repodata/repomd.xml

cp -a /root/.gnupg/key.public $RPM_DIR/repodata/repomd.xml.key
cp -a /root/.gnupg/key.public $SRPM_DIR/repodata/repomd.xml.key
