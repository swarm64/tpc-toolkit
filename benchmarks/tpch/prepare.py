
from swarm64_tpc_toolkit.prepare import PrepareBenchmarkFactory

class PrepareBenchmark(PrepareBenchmarkFactory):
    PrepareBenchmarkFactory.TABLES = (
        'region',
        'customer',
        'lineitem',
        'nation',
        'orders',
        'part',
        'partsupp',
        'supplier'
    )

    TABLE_CODES = {
        'region': 'r',
        'customer': 'c',
        'lineitem': 'L',
        'nation': 'n',
        'orders': 'O',
        'part': 'P',
        'partsupp': 'S',
        'supplier': 's'
    }
# ingest r region &
# ingest c customer $CHUNKS &
# ingest L lineitem $CHUNKS &
# ingest n nation &
# ingest O orders $CHUNKS &
# ingest P part $CHUNKS &
# ingest S partsupp $CHUNKS &
# ingest s supplier $CHUNKS &
    def ingest(self, table):
        use_chunks = table not in ('nation', 'region')

        table_code = PrepareBenchmark.TABLE_CODES[table]

        dbgen_cmd = f'./dbgen -s {self.args.scale_factor} -T {table_code} -o'
        psql_copy = f"psql {self.args.dsn} -c \"COPY {table} FROM STDIN WITH DELIMITER '|'\""
        total_chunks = 10

        if use_chunks:
            return [f'{dbgen_cmd} -S {chunk} -C {total_chunks} | {psql_copy}' for
                    chunk in range(1, total_chunks + 1)]

        return [f'{dbgen_cmd} | {psql_copy}']
        # function ingest {
        #     TABLE_CODE=$1
        #     TABLE=$2
        #     TOTAL_CHUNKS=$3

        #     echo "Copying $BENCHMARK SF${SCALE_FACTOR} data to ${TABLE}"

        #     PSQL_COPY="\COPY $TABLE FROM STDIN WITH DELIMITER '|'"
        #     DBGEN="./dbgen -s $SCALE_FACTOR -T $TABLE_CODE -o"

        #     if [ -z $TOTAL_CHUNKS ] || [ "$SCALE_FACTOR" -eq 1 ]; then
        #         echo "Using a single chunk."
        #         $DBGEN | psql_exec_cmd "$PSQL_COPY"
        #     else
        #         echo "Using multiple chunks."
        #         for CHUNK in `seq 1 $TOTAL_CHUNKS`; do
        #             echo "$TABLE: generating $CHUNK of $TOTAL_CHUNKS"
        #             DBGEN_CMD="$DBGEN -S $CHUNK -C $TOTAL_CHUNKS"
        #             $DBGEN_CMD | psql_exec_cmd "$PSQL_COPY" &
        #         done
        #         wait
        #     fi
        # }
