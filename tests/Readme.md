[см wiki](https://confluence.postgrespro.ru/display/DEV/pg_probackup)

```
Note: For now there are tests only for Linix
```


```
Check physical correctness of restored instances:
 Apply this patch to disable HINT BITS: https://gist.github.com/gsmol/2bb34fd3ba31984369a72cc1c27a36b6
 export PG_PROBACKUP_PARANOIA=ON

Check archive compression:
 export ARCHIVE_COMPRESSION=ON

Specify path to pg_probackup binary file. By default tests use <Path to Git repository>/pg_probackup/
 export PGPROBACKUPBIN=<path to pg_probackup>

Usage:
 pip install testgres
 pip install psycopg2
 export PG_CONFIG=/path/to/pg_config
 python -m unittest [-v] tests[.specific_module][.class.test]
```
