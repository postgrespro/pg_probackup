xsltproc test_pio-Results.xml > results-pio.html 2> /dev/null
xsltproc test_probackup-Results.xml > results-init.html 2> /dev/null
lcov -t "pb" --output pb.info --capture --directory . --directory ../s3 --directory ../src --rc lcov_branch_coverage=1  > /dev/null 2>&1
genhtml --output report --branch-coverage pb.info  > /dev/null 2>&1
xdg-open report/index.html > /dev/null 2>&1
xdg-open results-pio.html > /dev/null 2>&1
if test -s results-init.html ; then
  xdg-open results-init.html > /dev/null 2>&1
fi
