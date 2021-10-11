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

export INPUT_DIR=/app/in # dir with builded deb
export OUT_DIR=/app/www/${PBK_PKG_REPO}
#export REPO_DIR=/app/repo

cd $INPUT_DIR

export DEB_DIR=$OUT_DIR/deb
export KEYS_DIR=$OUT_DIR/keys
export CONF=/app/repo/${PBK_PKG_REPO}/conf
mkdir -p "$KEYS_DIR"
cp -av /app/repo/${PBK_PKG_REPO}/gnupg /root/.gnupg

rsync /app/repo/${PBK_PKG_REPO}/gnupg/key.public $KEYS_DIR/GPG-KEY-PG_PROBACKUP
echo -e 'User-agent: *\nDisallow: /' > $OUT_DIR/robots.txt

mkdir -p $DEB_DIR
cd $DEB_DIR
cp -av $CONF ./

# make remove-debpkg tool
echo -n "#!"        > remove-debpkg
echo "/bin/sh"      >> remove-debpkg
echo "CODENAME=\$1" >> remove-debpkg
echo "DEBFILE=\$2"  >> remove-debpkg
echo "DEBNAME=\`basename \$DEBFILE | sed -e 's/_.*//g'\`"   >> remove-debpkg
echo "reprepro --waitforlock 5 remove \$CODENAME \$DEBNAME" >> remove-debpkg
chmod +x remove-debpkg

#find $INPUT_DIR/ -name '*.changes' -exec reprepro -P optional -Vb . include ${CODENAME} {} \;
find $INPUT_DIR -name "*${CODENAME}*.deb" -exec ./remove-debpkg $CODENAME {} \;
find $INPUT_DIR -name "*${CODENAME}*.dsc" -exec reprepro --waitforlock 5 -i undefinedtarget --ignore=missingfile -P optional -S main -Vb . includedsc $CODENAME {} \;
find $INPUT_DIR -name "*${CODENAME}*.deb" -exec reprepro --waitforlock 5 -i undefinedtarget --ignore=missingfile -P optional -Vb . includedeb $CODENAME {} \;
reprepro export $CODENAME

rm -f remove-debpkg
rm -rf ./conf
rm -rf /root/.gnupg
