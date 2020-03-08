
import logging

import requests
import pandas


LOG = logging.getLogger()

class Netdata:
    def __init__(self, config):
        self.url = f"{config['url']}/api/v1/data"
        self.metrics = config['metrics']
        self.charts = config['charts']

    def _get_data(self, timerange, resolution):
        data = pandas.DataFrame()
        for chart, dimensions in self.charts.items():
            result = requests.get(self.url, params={
                'chart': chart,
                'after': timerange[0],
                'before': timerange[1],
                'dimensions': ','.join(dimensions),
                'gtime': resolution
            }).json()

            columns = ['time']
            columns.extend([f'{chart}.{dimension}' for dimension in dimensions])
            columns = [column.replace('.', '_') for column in columns]

            df = pandas.DataFrame(result['data'], columns=columns)
            df = df.set_index('time')
            data = pandas.concat([data, df], axis=1)

        data.index = pandas.to_datetime(data.index, unit='s')

        return data

    @classmethod
    def make_timestamp(cls, value):
        return int(value.timestamp())

    def _write_stats_impl(self, df, output):
        data = {}

        for _, row in df.iterrows():
            timerange = (
                Netdata.make_timestamp(row['timestamp_start']),
                Netdata.make_timestamp(row['timestamp_stop'])
            )

            netdata_df = self._get_data(timerange, 1)
            data[name] = netdata_df.agg(self.metrics)

        with open(output, 'w') as output_file:
            for name, df in data.items():
                output_file.write(f'{name}')
                df.to_csv(output_file)
                output_file.write('\n')

    def write_stats(self, df, output):
        if len(df['stream_id'].unique()) == 1:
            self._write_stats_impl(df, output)

        else:
            LOG.info('Running more than one stream. Not retrieving netdata stats.')

    def get_system_stats(self, df, resolution):
        ts_from = Netdata.make_timestamp(df['timestamp_start'].min())
        ts_to = Netdata.make_timestamp(df['timestamp_stop'].max())
        return self._get_data((ts_from, ts_to), resolution).sort_index()
