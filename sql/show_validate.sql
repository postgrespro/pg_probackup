-- test show command
\! rm -rf ${PWD}/results/sample_backup
\! cp -rp data/sample_backup ${PWD}/results/sample_backup
\! pg_rman show -B ${PWD}/results/sample_backup
\! pg_rman validate -B ${PWD}/results/sample_backup 2009-05-31 17:05:53 --debug
\! pg_rman validate -B ${PWD}/results/sample_backup 2009-06-01 17:05:53 --debug
\! pg_rman show -a -B ${PWD}/results/sample_backup
\! pg_rman show 2009-06-01 17:05:53 -B ${PWD}/results/sample_backup
