#!/bin/bash

# SCHEMA=$1
# NODE_CONFIG=$2
# DSN=$3

function print_help {
echo "
Usage instructions:
    $0 \\
      --dsn=<coordinator dsn> \\
      --schema=<master schema to use> \\
      --node-config=<node config to use>"
}

function check_and_fail {
    if [ -z ${!1+x} ]; then
        echo "ERROR: ${2} is not set."
        print_help
        exit -1
    fi
}

function check_program_and_fail {
    PROGRAM=$1
    HINT=$2
    if ! hash $PROGRAM 2> /dev/null; then
        echo "ERROR:    $PROGRAM    is not installed. $HINT"
        exit -1
    fi
}

for i in "$@"
do
case $i in
    --schema=*)
    SCHEMA="${i#*=}"
    shift
    ;;
    --dsn=*)
    DSN="${i#*=}"
    shift
    ;;
    --node-config=*)
    NODE_CONFIG="${i#*=}"
    shift
    ;;
    *)
        echo "Unknown option $i"
        print_help
        exit -1
    ;;
esac
done

check_and_fail SCHEMA "--schema"
check_and_fail DSN "--dsn"
check_and_fail NODE_CONFIG "--node-config"

check_program_and_fail "jinja2" "Did you run 'pip3 install -r requirements.txt'?"
check_program_and_fail "psql" "Is it installed? Is PATH setup properly?"

jinja2 $SCHEMA $NODE_CONFIG | psql $DSN
