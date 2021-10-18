#!/usr/bin/env bash

# Copyright Notice:
# © (C) Postgres Professional 2015-2016 http://www.postgrespro.ru/
# Distributed under Apache License 2.0
# Распространяется по лицензии Apache 2.0

set -exu
set -o errexit
set -o pipefail

# fix https://github.com/moby/moby/issues/23137
ulimit -n 1024

export INPUT_DIR=/app/in #dir with builded rpm
export OUT_DIR=/app/www/${PBK_PKG_REPO}

apt-get update -y
apt-get install -qq -y apt-repo-tools gnupg rsync perl less wget

if [[ ${PBK_EDITION} == '' ]] ; then
	REPO_SUFFIX='vanilla'
	FORK='PostgreSQL'
else
	REPO_SUFFIX='forks'
	FORK='PostgresPro'
fi

cd $INPUT_DIR

cp -arv /app/repo/$PBK_PKG_REPO/gnupg /root/.gnupg
chmod -R 0600 /root/.gnupg
for pkg in $(ls); do
	for pkg_full_version in $(ls ./$pkg); do

		# THere is no std/ent packages for PG 9.5
		if [[ ${pkg} == 'pg_probackup-std-9.5' ]] || [[ ${pkg} == 'pg_probackup-ent-9.5' ]] ; then
			continue;
		fi

		RPM_DIR=${OUT_DIR}/rpm/${pkg_full_version}/altlinux-p${DISTRIB_VERSION}/x86_64/RPMS.${REPO_SUFFIX}
		mkdir -p "$RPM_DIR"
		cp -arv $INPUT_DIR/$pkg/$pkg_full_version/RPMS/x86_64/* $RPM_DIR/

		genbasedir --architecture=x86_64 --architectures=x86_64 --origin=repo.postgrespro.ru \
		--label="${FORK} backup utility pg_probackup" --description "${FORK} pg_probackup repo" \
		--version=$pkg_full_version --bloat --progress --create \
		--topdir=${OUT_DIR}/rpm/${pkg_full_version}/altlinux-p${DISTRIB_VERSION} x86_64 ${REPO_SUFFIX}

		# SRPM is available only for vanilla
		if [[ ${PBK_EDITION} == '' ]] ; then
			SRPM_DIR=${OUT_DIR}/srpm/${pkg_full_version}/altlinux-p${DISTRIB_VERSION}/x86_64/SRPMS.${REPO_SUFFIX}
			mkdir -p "$SRPM_DIR"
			cp -arv $INPUT_DIR/$pkg/$pkg_full_version/SRPMS/* $SRPM_DIR/

			genbasedir --architecture=x86_64 --architectures=x86_64 --origin=repo.postgrespro.ru \
			--label="${FORK} backup utility pg_probackup sources" --description "${FORK} pg_probackup repo" \
			--version=$pkg_full_version --bloat --progress --create \
			--topdir=${OUT_DIR}/srpm/${pkg_full_version}/altlinux-p${DISTRIB_VERSION} x86_64 ${REPO_SUFFIX}
		fi
	done
done
