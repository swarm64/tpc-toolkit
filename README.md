# Summary

This toolset runs TPC-H and TPC-DS benchmarks.


# Prerequisites

- Python min. 3.6 and pip3
- Install additional packages with `pip3 install -r requirements.txt`
- The database can be accessed with user `postgres` *without password*


# Create a database and load data

There are two benchmarks available: TPC-H and TPC-DS.

1. To load a database with a dataset, go to the correct benchmark directory:
   For TPC-H: `cd schemas/tpch`
   For TPC-DS: `cd schemas/tpcds`

2. Run the `loader.sh` script with the following parameters:

    ./loader.sh \
        --schema=<schema-to-deploy> \
        --scale-factor=<scale-factor-to-use> \
        --dbname=<target-db>

   The required parameters are as follows:

   | Parameters     | Descriptions                                           |
   | -------------- | ------------------------------------------------------ |
   | `schema`       | The schema to deploy. Schemas are directories in the   |
   |                | current working directory and start with either `sdb_` |
   |                | or `psql_`. The schema name equals the directory name. |
   | `scale-factor` | The scale factor to use, such as `100` or `1000`.      |
   | `dbname`       | The name of the target database. If the database does  |
   |                | not exist, it will be created. If it does exist, it    |
   |                | will be deleted and recreated.                         |

   Additional parameters are as follows:

   | Parameters       | Descriptions                                         |
   | ---------------- | ---------------------------------------------------- |
   | `num-partitions` | The number of partitions to use, if applicable. The  |
   |                  | default is 32.                                       |
   | `chunks`         | Chunk large tables into smaller pieces during        |
   |                  | ingestion. Defaults to 10.                           |
   | `db-host`        | Alternative host for the database.                   |
   | `db-port`        | Alternative port for the database.                   |

   Depending on the scale factor you choose, the time it takes for the script
   to finish might take up to several hours. After the script creates the
   database, it loads the data, creates primary keys, foreign keys, and
   indices. Afterwards, it runs VACUUM and ANALYZE.


# Run a benchmark

Start a TPC-H or TPC-DS benchmark:

    ./swarm64_run_tpc_benchmark \
        --dsn postgresql://postgres@localhost/<target-db> \
        --benchmark <tpch|tpcds>

This runs the benchmark without any query runtime restriction. Ideally, use the
`--timeout` parameter to limit query runtime. Queries might otherwise run for
several hours or longer.

The minimum required parameters are as follows:

| Parameters  | Descriptions                                    |
| ----------- | ----------------------------------------------- |
| `dsn`       | The full DSN of the DB to connect to            |
| `benchmark` | The benchmark to use. Either `tpch` or `tpcds`. |

Additional parameters are as follows:

| Parameters | Descriptions                                       |
| ---------- | -------------------------------------------------- |
| `config`   | Path to additional YAML configuration file.        |
| `timeout`  | The maximum time a query may run, such as `15min`. |


# Test parameterization with additional YAML configuration

You can create an additional configuration file to control test execution more
granularly. An example YAML file is as follows:

    timeout: 30min
    ignore:
      - 20
      - 21
      - 22

    dbconfig:
      max_parallel_workers: 96
      max_parallel_workers_per_gather: 32

To use this file, pass the `--config=<path-to-file>` argument to the test
executor. In this example, the query timeout is set to `30min`. Queries 20, 21,
and 22 will not execute. Additionally, the database parameters
`max_parallel_workers` will change to 96 and `max_parallel_workers_per_gather`
will change to `32`. Any change to the database configuration is applied before
the benchmark starts and are reverted after the benchmark completes.
