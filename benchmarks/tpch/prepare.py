
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
