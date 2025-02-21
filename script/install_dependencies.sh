#!/bin/bash

PYTHON_VERSION=3.12
EXTRA_PARAMS=""

func() {
  echo "Usage:"
  echo "  install_dependencies.sh [-v PYTHON_VERSION] [-e EXTRA_PARAMS]"
  echo "Description:"
  echo "  -v: PYTHON_VERSION, python version, support: [3.8, 3.12]."
  echo "  -e: EXTRA_PARAMS, extended parameters for installation dependencies. e.g.: --retries 5 "
  exit 255
}

while getopts 'h:v:e:' OPT; do
  case $OPT in
  v) PYTHON_VERSION="$OPTARG" ;;
  e) EXTRA_PARAMS="$OPTARG" ;;
  h) func ;;
  ?) func ;;
  esac
done

function check_python_version() {
  if [[ "$PYTHON_VERSION" != "3.8" && "$PYTHON_VERSION" != "3.12" ]]; then
    echo "Not support PYTHON_VERSION: $PYTHON_VERSION, only support: [3.8, 3.12]."
    exit 255
  else
    echo "PYTHON_VERSION: $PYTHON_VERSION"
  fi
}

function run_cmd() {
  echo "Run cmd: $1"
  eval "$1"
}

main() {
  check_python_version

  run_cmd "pwd && ls -l ../dependencies/"
  run_cmd "python$PYTHON_VERSION -m pip install --upgrade pip $EXTRA_PARAMS"
  run_cmd "python$PYTHON_VERSION -m pip install -r ../dependencies/requirements_$PYTHON_VERSION.txt $EXTRA_PARAMS"
}

main
