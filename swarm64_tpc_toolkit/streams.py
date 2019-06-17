
import logging
import os

from multiprocessing import Pool

import pandas
import yaml

from .db import DB


LOG = logging.getLogger()


class Streams:
    def __init__(self, args):
        self.config = Streams._make_config(args)
        self.db = DB(args.dsn)
        self.num_streams = args.streams
        self.netdata_url = args.netdata_url
        self.query_dir = os.path.join('queries', args.benchmark)
        self.stream_offset = args.stream_offset
        self.output = args.output

    @staticmethod
    def _make_config(args):
        config = {}
        if args.config:
            with open(args.config, 'r') as config_file:
                config = yaml.load(config_file, Loader=yaml.Loader)

        if args.timeout:
            config['timeout'] = args.timeout

        return config

    def read_sql_file(self, query_id):
        query_path = os.path.join(self.query_dir, f'{query_id}.sql')
        with open(query_path, 'r') as query_file:
            return query_file.read()

    @staticmethod
    def apply_sql_modifications(sql, modifications):
        for modification in modifications:
            sql = sql.replace(modification[0], modification[1])
        return sql

    def _print_results(self, results):
        df = pandas.DataFrame()

        for column in results:
            key = list(column.keys())[0]
            columns = [f'{key} start', f'{key} stop', f'{key} status']

            _df = pandas.DataFrame(data=column[key]).transpose().sort_index()
            _df.columns = columns

            df[f'Stream {key:02} metric'] = (_df[columns[1]] - _df[columns[0]]).apply(lambda x: round(x, 2))
            df[f'Stream {key:02} status'] = _df[columns[2]].transform(lambda x: x.name)

        df.index = _df.index
        df.index.name = 'Query'

        if self.output == 'print':
            with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
                print(df)
        elif self.output == 'csv':
            print(df.to_csv(sep=';'))
        else:
            raise ValueError(f'Unknown output format {self.output}')

    def run(self):
        try:
            self.db.reset_config()
            self.db.apply_config(self.config.get('dbconfig', {}))

            results = self.run_streams()
            self._print_results(results)

        except KeyboardInterrupt:
            # Reset all the stuff
            pass

        finally:
            self.db.reset_config()

    def get_stream_sequence(self, stream_id):
        streams_path = os.path.join(self.query_dir, 'streams.yaml')
        with open(streams_path, 'r') as streams_file:
            return yaml.load(streams_file, Loader=yaml.Loader)[stream_id]

    def _make_run_args(self):
        if self.num_streams == 0:
            return ((0,),)
        else:
            return tuple((stream,) for stream in range(self.stream_offset, self.num_streams + self.stream_offset))

    def run_streams(self):
        with Pool(processes=max(self.num_streams, 1)) as pool:
            map_args = self._make_run_args()
            return pool.starmap(self._run_stream, map_args)

    def _run_stream(self, stream_id):
        sequence = self.get_stream_sequence(stream_id)

        timings = {}
        num_queries = len(sequence)
        for idx, query_id in enumerate(sequence):
            num_query = idx + 1
            pretext = f'{num_query:2}/{num_queries:2}: query {query_id:2} of stream {stream_id:2}'

            if query_id in self.config.get('ignore', []):
                LOG.info(f'ignoring {pretext}.')
                continue

            query_sql = self.read_sql_file(query_id)
            query_sql = Streams.apply_sql_modifications(query_sql, (('revenue0', f'revenue{stream_id}'),))

            LOG.info(f'running  {pretext}.')
            timing = self.db.run_query(query_sql, self.config.get('timeout', 0))

            runtime = round(timing.stop - timing.start, 2)
            LOG.info(f'finished {pretext}: {runtime:7.2f} - {timing.status.name}')

            timings[query_id] = timing

        return {stream_id: timings}