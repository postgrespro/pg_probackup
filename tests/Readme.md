[см wiki](https://confluence.postgrespro.ru/display/DEV/pg_probackup)

```
Note: For now these are works on Linux and "kinda" works on Windows
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

Run suit of basic simple tests:
 export PG_PROBACKUP_TEST_BASIC=ON

Run ptrack tests:
 export PG_PROBACKUP_PTRACK=ON


Usage:
 pip install testgres
 export PG_CONFIG=/path/to/pg_config
 python -m unittest [-v] tests[.specific_module][.class.test]
```
