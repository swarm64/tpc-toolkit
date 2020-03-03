#!/bin/bash

cd "$(dirname "$(readlink -f "$0")")"
source ../../scripts/functions.sh

function ingest {
    TABLE_CODE=$1
    TABLE=$2
    TOTAL_CHUNKS=$3

    echo "Copying $BENCHMARK SF${SCALE_FACTOR} data to ${TABLE}"

    PSQL_COPY="\COPY $TABLE FROM STDIN WITH DELIMITER '|'"
    DBGEN="./dbgen -s $SCALE_FACTOR -T $TABLE_CODE -o"

    if [ -z $TOTAL_CHUNKS ] || [ "$SCALE_FACTOR" -eq 1 ]; then
        echo "Using a single chunk."
        $DBGEN | psql_exec_cmd "$PSQL_COPY"
    else
        echo "Using multiple chunks."
        for CHUNK in `seq 1 $TOTAL_CHUNKS`; do
            echo "$TABLE: generating $CHUNK of $TOTAL_CHUNKS"
            DBGEN_CMD="$DBGEN -S $CHUNK -C $TOTAL_CHUNKS"
            $DBGEN_CMD | psql_exec_cmd "$PSQL_COPY" &
        done
        wait
    fi
}

function run {
    execute_parallel \
        "ingest r region" \
        "ingest c customer $CHUNKS" \
        "ingest L lineitem $CHUNKS" \
        "ingest n nation" \
        "ingest O orders $CHUNKS" \
        "ingest P part $CHUNKS" \
        "ingest S partsupp $CHUNKS" \
        "ingest s supplier $CHUNKS"

    run_if_exists primary-keys.sql
    run_if_exists foreign-keys.sql
    run_if_exists indexes.sql

    psql_vacuum

    execute_parallel \
        "psql_analyze region" \
        "psql_analyze customer" \
        "psql_analyze lineitem" \
        "psql_analyze nation" \
        "psql_analyze orders" \
        "psql_analyze part" \
        "psql_analyze partsupp" \
        "psql_analyze supplier"
}

wait_for_pg

prepare_db UTF8
deploy_schema "$SCHEMA" "$NUM_PARTITIONS"

measure "Ingest runtime" run
