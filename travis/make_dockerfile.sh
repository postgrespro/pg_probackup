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

if [ -z ${APPLY_PTRACK_PATCH+x} ]; then
	APPLY_PTRACK_PATCH=off
fi

echo PG_VERSION=${PG_VERSION}
echo PG_BRANCH=${PG_BRANCH}
echo MODE=${MODE}
echo APPLY_PTRACK_PATCH=${APPLY_PTRACK_PATCH}

sed \
	-e 's/${PG_VERSION}/'${PG_VERSION}/g \
	-e 's/${PG_BRANCH}/'${PG_BRANCH}/g \
	-e 's/${MODE}/'${MODE}/g \
	-e 's/${APPLY_PTRACK_PATCH}/'${APPLY_PTRACK_PATCH}/g \
Dockerfile.in > Dockerfile
