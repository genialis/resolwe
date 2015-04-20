#!/bin/bash
#
# A script for running Resolwe tests with Jenkins.
#
# To use this script, add an "Execute shell" "Build" step to your project and
# put in the following:
# ./tests/run_tests.sh
#

pushd $WORKSPACE

VENV_HOME=$WORKSPACE/.venv
[ ! -d $VENV_HOME ] && rm -rf $VENV_HOME
virtualenv $VENV_HOME
. $VENV_HOME/bin/activate
pip install -U pip
python setup.py install

./tests/manage.py makemigrations
./tests/manage.py migrate
./tests/manage.py jenkins resolwe --enable-coverage

cloc --exclude-dir=.venv,reports, --by-file --xml --out=reports/cloc.xml .

popd
