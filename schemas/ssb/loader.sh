#!/bin/bash

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
        $DBGEN | sed 's/|$//' | psql_exec_cmd "$PSQL_COPY"
    else
        echo "Using multiple chunks."
        for CHUNK in `seq 1 $TOTAL_CHUNKS`; do
            echo "$TABLE: generating $CHUNK of $TOTAL_CHUNKS"
            DBGEN="$DBGEN -S $CHUNK -C $TOTAL_CHUNKS"
            $DBGEN | sed 's/|$//' | psql_exec_cmd "$PSQL_COPY" &
        done
        wait
    fi
}

wait_for_pg

prepare_db UTF8
deploy_schema "$SCHEMA" "$NUM_PARTITIONS"

ingest d date &
ingest c customer $CHUNKS &
ingest p part &
ingest s supplier &
ingest l lineorder $CHUNKS & 

wait

run_if_exists primary-keys.sql
run_if_exists foreign-keys.sql
run_if_exists indexes.sql

psql_exec_cmd "VACUUM"

psql_exec_cmd "ANALYZE date" &
psql_exec_cmd "ANALYZE customer" &
psql_exec_cmd "ANALYZE lineorder" &
psql_exec_cmd "ANALYZE supplier" &
psql_exec_cmd "ANALYZE part" &

wait
