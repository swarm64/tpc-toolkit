#!/bin/bash

source ../../scripts/functions.sh

function stream_to_db {
    grep ^${1} | sed -r 's/^'${1}' (.*)/\1/' | psql_exec_cmd "COPY ${1} FROM STDIN WITH (FORMAT CSV, DELIMITER '|')"
}

function ingest_impl {
    TABLE=$1
    CHUNK=$2
    CHUNKS=$3

    DBGEN="./dsdgen -SCALE $SCALE_FACTOR -TABLE $TABLE -RNGSEED 1 -TERMINATE N -FILTER Y"
    if [ ! -z $CHUNK ]; then
        DBGEN="${DBGEN} -PARALLEL ${CHUNKS} -CHILD ${CHUNK}"
    fi

    if [[ $TABLE = *"_sales"* ]]; then
        # Has also a returns table
        RETURNS_TABLE="$(echo $TABLE | cut -d'_' -f1)_returns"
        $DBGEN | recode ISO-8859-1..UTF-8 | tee >(stream_to_db ${TABLE}) | (stream_to_db ${RETURNS_TABLE})
    else
        $DBGEN | recode ISO-8859-1..UTF-8 | stream_to_db ${TABLE}
    fi
}

function ingest {
    TABLE=$1
    TOTAL_CHUNKS=$2

    BENCHMARK="TPC-DS"

    if [ -z $TOTAL_CHUNKS ] || [ "$SCALE_FACTOR" -lt 100 ]; then
        echo "Copying $BENCHMARK SF${SCALE_FACTOR} data to ${TABLE}"
        ingest_impl $TABLE
    else
        for CHUNK in `seq 1 $TOTAL_CHUNKS`; do
            echo "Copying $BENCHMARK SF${SCALE_FACTOR} data to ${TABLE} chunk ${CHUNK} of ${TOTAL_CHUNKS}"
            ingest_impl $TABLE $CHUNK $TOTAL_CHUNKS &
        done
        wait
    fi
}

check_program_and_fail "recode"

wait_for_pg

prepare_db UTF8
deploy_schema "$SCHEMA" "$NUM_PARTITIONS"

ingest customer_address &
ingest customer_demographics &
ingest date_dim &
ingest warehouse &
ingest ship_mode &
ingest time_dim &
ingest reason &
ingest income_band &
ingest item &
ingest store &
ingest call_center &
ingest customer &
ingest web_site &
ingest household_demographics &
ingest web_page &
ingest promotion &
ingest catalog_page &
ingest inventory $CHUNKS &
ingest web_sales $CHUNKS &
ingest catalog_sales $CHUNKS &
ingest store_sales $CHUNKS &

wait

run_if_exists primary-keys.sql
run_if_exists foreign-keys.sql
run_if_exists indexes.sql

psql_exec_cmd "VACUUM"

psql_exec_cmd "ANALYZE customer_address"
psql_exec_cmd "ANALYZE customer_demographics"
psql_exec_cmd "ANALYZE date_dim"
psql_exec_cmd "ANALYZE warehouse"
psql_exec_cmd "ANALYZE ship_mode"
psql_exec_cmd "ANALYZE time_dim"
psql_exec_cmd "ANALYZE reason"
psql_exec_cmd "ANALYZE income_band"
psql_exec_cmd "ANALYZE item"
psql_exec_cmd "ANALYZE store"
psql_exec_cmd "ANALYZE call_center"
psql_exec_cmd "ANALYZE customer"
psql_exec_cmd "ANALYZE web_site"
psql_exec_cmd "ANALYZE household_demographics"
psql_exec_cmd "ANALYZE web_page"
psql_exec_cmd "ANALYZE promotion"
psql_exec_cmd "ANALYZE catalog_page"
psql_exec_cmd "ANALYZE inventory"
psql_exec_cmd "ANALYZE web_sales"
psql_exec_cmd "ANALYZE catalog_sales"
psql_exec_cmd "ANALYZE store_sales"
