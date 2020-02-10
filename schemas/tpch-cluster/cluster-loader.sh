#!/bin/bash

source ../../scripts/functions-cluster.sh

function ingest {
    TABLE_CODE=$1
    TABLE=$2
    TOTAL_CHUNKS=$3

    echo "Copying $BENCHMARK SF${SCALE_FACTOR} data to ${TABLE}"

    NUM_NODES=${#NODES[@]}
    PSQL_COPY="COPY $TABLE FROM STDIN WITH DELIMITER '|'"
    DBGEN="./dbgen -s $SCALE_FACTOR -T $TABLE_CODE -o"
    SEND="./sender.py --rmq-dsn $RMQ_DSN --receivers $NUM_NODES --exchange tpch_data --rpc-exchange rpc --table $TABLE --db $DB --data-source stdin --flush-size $((NUM_NODES*100000))"

    if [ -z $TOTAL_CHUNKS ] || [ "$SCALE_FACTOR" -eq 1 ]; then
        echo "Using a single chunk for $TABLE stream ID: $OFFSET"
        $DBGEN | sed -r 's/^([0-9]+)(.*)\|$/\1 \1\2/' | $SEND
    else
        echo "Using multiple chunks."
        for CHUNK in `seq 1 $TOTAL_CHUNKS`; do
            echo "$TABLE: generating $CHUNK of $TOTAL_CHUNKS"

            DBGEN_CMD="$DBGEN -S $CHUNK -C $TOTAL_CHUNKS"
            $DBGEN_CMD | sed -r 's/^([0-9]+)(.*)\|$/\1 \1\2/' | $SEND &
        done
        wait
    fi
}

function create_schema {
    NODE=$1
    IDX=$2
    echo $NODE $IDX

    echo $NODE
    wait_for_pg $NODE
    prepare_db $NODE UTF8
    deploy_schema "$NODE" "$IDX" "${#NODES[@]}" "$SCHEMA" "$NUM_PARTITIONS"
    echo $NODE
}

function run_on_nodes_parallel {
    echo "$1"
    for NODE in "${NODES[@]}"; do
        eval "$1 $NODE" &
    done
    wait
}

for IDX in "${!NODES[@]}"; do
    create_schema ${NODES[IDX]} $IDX &
done
wait

ingest r region &
ingest n nation &

wait

ingest c customer $CHUNKS
ingest L lineitem $CHUNKS
ingest O orders $CHUNKS
ingest P part $CHUNKS
ingest S partsupp $CHUNKS
ingest s supplier $CHUNKS

run_on_nodes_parallel "run_if_exists primary-keys.sql"
run_on_nodes_parallel "run_if_exists foreign-keys.sql"
run_on_nodes_parallel "run_if_exists indexes.sql"

run_on_nodes_parallel "psql_exec_cmd 'VACUUM'"
run_on_nodes_parallel "psql_exec_cmd 'ANALYZE VERBOSE region'" &
run_on_nodes_parallel "psql_exec_cmd 'ANALYZE VERBOSE customer'" &
run_on_nodes_parallel "psql_exec_cmd 'ANALYZE VERBOSE lineitem'" &
run_on_nodes_parallel "psql_exec_cmd 'ANALYZE VERBOSE nation'" &
run_on_nodes_parallel "psql_exec_cmd 'ANALYZE VERBOSE orders'" &
run_on_nodes_parallel "psql_exec_cmd 'ANALYZE VERBOSE part'" &
run_on_nodes_parallel "psql_exec_cmd 'ANALYZE VERBOSE partsupp'" &
run_on_nodes_parallel "psql_exec_cmd 'ANALYZE VERBOSE supplier'" &

wait
