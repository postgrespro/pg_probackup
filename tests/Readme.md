****[see wiki](https://confluence.postgrespro.ru/display/DEV/pg_probackup)

```
Note: For now these tests work on Linux and "kinda" work on Windows
```

```
Note: tests require python3 to work properly.
```

```
Windows Note: For tablespaces tests to work on Windows, you should explicitly(!) grant current user full access to tmp_dirs
```


```
Check physical correctness of restored instances:
 Apply this patch to disable HINT BITS: https://gist.github.com/gsmol/2bb34fd3ba31984369a72cc1c27a36b6
 export PG_PROBACKUP_PARANOIA=ON

Check archive compression:
 export ARCHIVE_COMPRESSION=ON

Enable compatibility tests:
 export PGPROBACKUPBIN_OLD=/path/to/previous_version_pg_probackup_binary

Specify path to pg_probackup binary file. By default tests use <Path to Git repository>/pg_probackup/
 export PGPROBACKUPBIN=<path to pg_probackup>

Remote backup depends on key authentication to local machine via ssh as current user.
 export PGPROBACKUP_SSH_REMOTE=ON

Run tests that are relied on advanced debugging features. For this mode, pg_probackup should be compiled without optimizations. For example:
CFLAGS="-O0" ./configure --prefix=/path/to/prefix --enable-debug --enable-cassert --enable-depend --enable-tap-tests --enable-nls

 export PGPROBACKUP_GDB=ON

Run suit of basic simple tests:
 export PG_PROBACKUP_TEST_BASIC=ON

Run ptrack tests:
 export PG_PROBACKUP_PTRACK=ON

Run long (time consuming) tests:
 export PG_PROBACKUP_LONG=ON

Usage:
 sudo echo 0 > /proc/sys/kernel/yama/ptrace_scope
 pip install testgres
 export PG_CONFIG=/path/to/pg_config
 python -m unittest [-v] tests[.specific_module][.class.test]
```

# Troubleshooting FAQ

## Python tests failure
### 1. Could not open extension "..."
```
testgres.exceptions.QueryException ERROR:  could not open extension control file "<postgres_build_dir>/share/extension/amcheck.control": No such file or directory
```

#### Solution:

You have no `<postgres_src_root>/contrib/...` extension installed, please do

```commandline
cd <postgres_src_root>
make install-world
```
