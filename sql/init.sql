\! rm -rf results/init_test
\! pg_rman init -B ${PWD}/results/init_test --quiet;echo $?
\! find results/init_test | xargs ls -Fd | sort
\! pg_rman init -B ${PWD}/results/init_test --quiet;echo $?
