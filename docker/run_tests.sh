#!/usr/bin/env bash

# A simple shell script that is meant to be run inside a docker container for running hopsworks-cloud-sdk unit tests
# The script will take an argument that is the python version (supported versions currently are : 3.6)
# the virtual env corresponding to the python version will be activated and then the unit tests will be run inside
# that environment
# example usage:
# ./run.sh 3.6

PYTHON_VER=$1

if [[ "${PYTHON_VER}" != "3.6"]]
then
  echo "Invalid python version, supported versions are : 3.6"
  exit 1
fi

echo "Running Unit Tests with Python ${PYTHON_VER}"

source /hops_venv${PYTHON_VER}/bin/activate
cd /hops
pip install -e .
pytest -v hops $2
exit 0