#!/bin/bash
#
# A script for running Resolwe tests with Jenkins.
#
# To use this script, add an "Execute shell" "Build" step to your project and
# put in the following:
# ./tests/run_tests.sh
#

set -x

pushd $WORKSPACE

rm -rf reports

TOX_CMD='tox -r'
if [[ $TOXENV == "py33" ]]; then
    echo "Enabeling SCL Python 3.3"
    scl enable python33 "$TOX_CMD"
else
    $TOX_CMD
fi

cloc --exclude-dir=.venv,.tox,reports, --by-file --xml --out=reports/cloc.xml .

popd
