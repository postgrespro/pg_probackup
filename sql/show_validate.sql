-- test show command
\! rm -rf ${PWD}/results/sample_backup
\! cp -rp data/sample_backup ${PWD}/results/sample_backup
\! pg_arman show -A ${PWD}/results/arclog -B ${PWD}/results/sample_backup
\! pg_arman validate -B ${PWD}/results/sample_backup 2009-05-31 17:05:53 --debug
\! pg_arman validate -B ${PWD}/results/sample_backup 2009-06-01 17:05:53 --debug
\! pg_arman show -a -A ${PWD}/results/arclog -B ${PWD}/results/sample_backup
\! pg_arman show 2009-06-01 17:05:53 -A ${PWD}/results/arclog -B ${PWD}/results/sample_backup
