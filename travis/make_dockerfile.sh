#!/usr/bin/env sh

if [ -z ${PG_VERSION+x} ]; then
	echo PG_VERSION is not set!
	exit 1
fi

if [ -z ${PG_BRANCH+x} ]; then
	echo PG_BRANCH is not set!
	exit 1
fi

if [ -z ${MODE+x} ]; then
	MODE=basic
fi

if [ -z ${PTRACK_PATCH_PG_VERSION+x} ]; then
	PTRACK_PATCH_PG_VERSION=off
fi

echo PG_VERSION=${PG_VERSION}
echo PG_BRANCH=${PG_BRANCH}
echo MODE=${MODE}
echo PTRACK_PATCH_PG_VERSION=${PTRACK_PATCH_PG_VERSION}

sed \
	-e 's/${PG_VERSION}/'${PG_VERSION}/g \
	-e 's/${PG_BRANCH}/'${PG_BRANCH}/g \
	-e 's/${MODE}/'${MODE}/g \
	-e 's/${PTRACK_PATCH_PG_VERSION}/'${PTRACK_PATCH_PG_VERSION}/g \
Dockerfile.in > Dockerfile
