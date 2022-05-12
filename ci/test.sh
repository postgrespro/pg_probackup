#!/usr/bin/env sh

if [ -z ${MODE+x} ]; then
	MODE=basic
fi

if [ -z ${PGPROBACKUP_GDB+x} ]; then
	PGPROBACKUP_GDB=ON
fi

export PATH=$PATH:$PG_BASE/bin
export LD_LIBRARY_PATH=$PG_BASE/lib

echo "############### pg_config path:"
which pg_config

echo "############### pg_config:"
pg_config

echo "############### Kernel parameters:"
sudo sysctl kernel.yama.ptrace_scope=0

echo "############### Testing..."
echo PG_PROBACKUP_PARANOIA=${PG_PROBACKUP_PARANOIA}
echo ARCHIVE_COMPRESSION=${ARCHIVE_COMPRESSION}
echo PGPROBACKUPBIN_OLD=${PGPROBACKUPBIN_OLD}
echo PGPROBACKUPBIN=${PGPROBACKUPBIN}
echo PGPROBACKUP_SSH_REMOTE=${PGPROBACKUP_SSH_REMOTE}
echo PGPROBACKUP_GDB=${PGPROBACKUP_GDB}
echo PG_PROBACKUP_PTRACK=${PG_PROBACKUP_PTRACK}

if [ "$MODE" = "basic" ]; then
    export PG_PROBACKUP_TEST_BASIC=ON
    echo PG_PROBACKUP_TEST_BASIC=${PG_PROBACKUP_TEST_BASIC}
    python3 -m unittest -v tests
else
    echo PG_PROBACKUP_TEST_BASIC=${PG_PROBACKUP_TEST_BASIC}
    python3 -m unittest -v tests.$MODE
fi

