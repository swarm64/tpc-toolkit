#!/bin/bash

cd "$(dirname "$(readlink -f "$0")")"
cd ../../

source ./scripts/functions.sh

./prepare_benchmark \
    --dsn=postgresql://postgres@${DB_HOST}/${DB} \
    --scale-factor=${SCALE_FACTOR} \
    --schema=${SCHEMA} \
    --benchmark=tpcds
