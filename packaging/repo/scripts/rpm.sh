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
chmod +x /app/repo/$PBK_PKG_REPO/autosign.sh
echo -e 'User-agent: *\nDisallow: /' > $OUT_DIR/robots.txt

cd $INPUT_DIR

cp -arv /app/repo/$PBK_PKG_REPO/rpmmacros /root/.rpmmacros
cp -arv /app/repo/$PBK_PKG_REPO/gnupg /root/.gnupg
chmod -R 0600 /root/.gnupg
chown -R root:root /root/.gnupg

for pkg in $(ls ${INPUT_DIR}); do
	for pkg_full_version in $(ls ${INPUT_DIR}/$pkg); do

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
	done
done

#	if [ $repo_name == 'pg_probackup-forks' ]
#	then 
#		#/app/www/pg_probackup_repo/
#		export KEYS_DIR=$OUT_DIR/$repo_name/keys
#		mkdir -p "$KEYS_DIR"
#		rsync /app/src/$repo_name/gnupg/key.public $KEYS_DIR/GPG-KEY-PG_PROBACKUP
#		chmod 755 $KEYS_DIR
#		chmod +x /app/src/$repo_name/autosign.sh
#		echo -e 'User-agent: *\nDisallow: /' > $OUT_DIR/$repo_name/robots.txt
#
#		cp -arv /app/src/$repo_name/rpmmacros /root/.rpmmacros
#		cp -arv /app/src/$repo_name/gnupg /root/.gnupg
#		chmod -R 0600 /root/.gnupg
#		
#		cd $INPUT_DIR/$repo_name
#		for pkg in $(ls); do
#			for pkg_full_version in $(ls ./$pkg); do
#
#				if [ $pkg == 'pg_probackup-repo-forks' ]; then
#					cp $INPUT_DIR/$repo_name/$pkg/$pkg_full_version/RPMS/noarch/pg_probackup-repo-*.noarch.rpm \
#						$KEYS_DIR/pg_probackup-repo-forks-$DISTRIB.noarch.rpm
#					continue
#			    fi
#
#				[ ! -z "$CODENAME" ] && export DISTRIB_VERSION=$CODENAME
#				RPM_DIR=$OUT_DIR/$repo_name/rpm/$pkg_full_version/${DISTRIB}-${DISTRIB_VERSION}-x86_64
##				SRPM_DIR=$OUT_DIR/$repo_name/srpm/$pkg_full_version/${DISTRIB}-${DISTRIB_VERSION}-x86_64
#
##				rm -rf "$RPM_DIR" && mkdir -p "$RPM_DIR"
##				rm -rf "$SRPM_DIR" && mkdir -p "$SRPM_DIR"
#				mkdir -p "$RPM_DIR"
##				mkdir -p "$SRPM_DIR"
#
#				cp -arv $INPUT_DIR/$repo_name/$pkg/$pkg_full_version/RPMS/x86_64/* $RPM_DIR/
##				cp -arv $INPUT_DIR/$repo_name/$pkg/$pkg_full_version/SRPMS/* $SRPM_DIR/
#
#				for f in $(ls $RPM_DIR/*.rpm); do rpm --addsign $f || exit 1; done
##				for f in $(ls $SRPM_DIR/*.rpm); do rpm --addsign $f || exit 1; done
#				createrepo $RPM_DIR/
##				createrepo $SRPM_DIR/
#			done
#		done
#
#		# repo cleanup
#		rm -rf /root/.rpmmacros
#		rm -rf /root/.gnupg
#	fi
