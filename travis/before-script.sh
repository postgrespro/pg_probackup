#!/usr/bin/env bash

set -xe

/etc/init.d/ssh start

# Show pg_config path (just in case)
echo "############### pg_config path:"
which pg_config

# Show pg_config just in case
echo "############### pg_config:"
pg_config

# Show kernel parameters
echo "############### kernel params:"
cat /proc/sys/kernel/yama/ptrace_scope
sudo sysctl kernel.yama.ptrace_scope=0
cat /proc/sys/kernel/yama/ptrace_scope
