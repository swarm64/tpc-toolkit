
import logging

import requests
import pandas


LOG = logging.getLogger()

FPGA_KEY = 'fpga.fpga_data_transfer'

BASE_COLUMNS = ['stream_id', 'query', 'query_time', 'fpga_time']
STATS_METRICS = ['min', 'max', 'mean', 'median']


class Stats:
    def __init__(self, netdata_url, disk):
        self.netdata_url = netdata_url
        self.disk = disk
        self.charts = {
            'system.cpu': {
                'metrics': ['user', 'system', 'iowait'],
            },
            'system.ram': {
                'metrics': ['free', 'used', 'cached'],
            },
            f'disk.{self.disk}': {
                'metrics': ['reads', 'writes'],
            },
            FPGA_KEY: {
                'metrics': ['host_to_fpga_byte_count', 'fpga_to_host_byte_count'],
            }
        }
        self.chart_ids = list(self.charts.keys())

    @staticmethod
    def make_columns(metrics):
        columns = BASE_COLUMNS
        for stats_metric in STATS_METRICS:
            columns.extend([f'{stats_metric}_{metric}' for metric in metrics])
        return columns

    def query_netdata(self, start, end):
        data = {}
        for chart in self.chart_ids:
            params = {
                'chart': chart,
                'format': 'json',
                'after': start,
                'before': end
            }
            response = requests.get(f'{self.netdata_url}/api/v1/data', params=params)
            data[chart] = response.json()

        # TODO: assert all have same number of rows
        return data

    def transform(self, data):
        header = []
        for chart_id in self.chart_ids:
            labels = data[chart_id]['labels']
            header.append([f'{chart_id}.{label}' for label in labels])

        def flatten(l):
            return [y for x in l for y in x]

        N = len(data[self.chart_ids[0]]['data'])
        alldata = [flatten(header)]
        for n in range(N):
            idx = N - n - 1
            row = flatten([data[chart]['data'][idx] for chart in self.charts])
            alldata.append(row)

        return alldata

    def get_chart_metrics(self):
        metrics = []
        for chart, items in self.charts.items():
            chart_metrics = [f'{chart}.{metric}' for metric in items['metrics']]
            metrics.extend(chart_metrics)
        return metrics

    def get_data_from_netdata(self, timing):
        netdata = self.query_netdata(timing.start, timing.stop)
        netdata = self.transform(netdata)
        return pandas.DataFrame(netdata[1:], columns=netdata[0])

    def extract_fpga_data(self, data):
        host_to_fpga_metric = f'{FPGA_KEY}.{self.charts[FPGA_KEY]["metrics"][0]}'
        return pandas.DataFrame({
            'data': data[host_to_fpga_metric]
        })

    def construct_row(self, stream_id, query, timing, netdata, fpga_data, metrics, columns):
        fpga_time = fpga_data[fpga_data['data'] > 0].count()['data']
        query_time = timing.stop - timing.start

        return pandas.DataFrame([[
            stream_id, query, query_time, fpga_time,
            *(netdata.min()[metrics]),
            *(netdata.max()[metrics]),
            *(netdata.mean()[metrics]),
            *(netdata.median()[metrics])
        ]], columns=columns)

    def make_data_from_results(self, columns, metrics, results):
        data = pandas.DataFrame([], columns=columns)
        for result in results:
            stream_id = list(result)[0]
            result = result[stream_id]
            for query, timing in result.items():
                netdata = self.get_data_from_netdata(timing)
                fpga_data = self.extract_fpga_data(netdata)
                new_row = Stats.construct_row(stream_id, query, timing, netdata, fpga_data, metrics, columns)

                data = data.append(new_row)

        return data

    def get_csv_stats(self, results):
        metrics = self.get_chart_metrics()
        columns = Stats._make_columns(metrics)
        data = self.make_data_from_results(columns, metrics, results)
        return data.to_csv(index=False, sep=' ')
