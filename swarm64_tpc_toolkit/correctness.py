import logging
import numpy
import os
import pandas as pd

from pandas.io.formats.style import Styler

LOG = logging.getLogger()

class Correctness:
    def __init__(self, scale_factor, benchmark):
        self.scale_factor = scale_factor
        self.query_output_folder = os.path.join('results', 'query_results')
        self.correctness_results_folder = os.path.join('correctness_results',
                                                       benchmark, f'sf{self.scale_factor}')

        self.html = ''
        self.diff = None

    def get_correctness_filepath(self, query_id):
        filepath = os.path.join(self.correctness_results_folder, f'{query_id}.csv')
        return filepath

    def has_differences(self, first_df, second_df):

        if first_df.empty != second_df.empty:
            return True

        first_df = first_df.round(2)
        second_df = second_df.round(2)

        for column in first_df:
            a = first_df[column].sort_values()
            b = second_df[column].sort_values()

            if a.dtype == 'float64':
                # absolute(a - b) <= (atol + rtol * absolute(b))
                if not numpy.isclose(a, b, rtol=1e-12, atol=0).all():
                    return True

            else:
                if not a.equals(b):
                    return True

        return False

    def check_correctness(self, stream_id, query_number):

        LOG.debug(f'Checking Stream={stream_id}, Query={query_number}')
        correctness_path = self.get_correctness_filepath(query_number)
        benchmark_path = os.path.join(self.query_output_folder, f'{stream_id}_{query_number}.csv')

        # Reading Correctness results
        try:
            correctness_result = pd.read_csv(correctness_path, float_precision='round_trip')
        except pd.errors.EmptyDataError:
            LOG.debug(f'Query {query_number} is empty in correctness results.')
            correctness_result = pd.DataFrame(columns=['col'])
        except FileNotFoundError:
            LOG.debug(f'Correctness results for {query_number} not found. Skipping correctness checking.')
            return 'OK'

        # Reading Benchmark results
        try:
            benchmark_result = pd.read_csv(benchmark_path, float_precision='round_trip')
        except pd.errors.EmptyDataError:
            LOG.debug(f'{stream_id}_{query_number}.csv empty in benchmark results.')
            benchmark_result = pd.DataFrame(columns=['col'])
        except FileNotFoundError:
            msg = f'Query results for {stream_id}-{query_number} not found. Reporting as mismatch.'
            LOG.debug(msg)
            self.html += f'<p>{msg}</p>'
            return 'Mismatch'

        if self.has_differences(benchmark_result, correctness_result):
            self.html += Correctness.to_html(self.diff,
                                             table_title=f'Mismatch in StreamId={stream_id}, Query={query_number}')
            return 'Mismatch'

        return 'OK'

    @staticmethod
    def to_html(df, table_title):

        def highlight_difference(data, color='yellow'):
            """

            :param data: The data frame containing differences
                            between benchmark and correctness results.
            :param color: The color to highlight the differences.
            :return: returns the highlighted data frame style.

            If one of the results has additional rows, then the way to compare them is
            1) to divide data frames into 2 parts
            2) compare the part of data frames that have the same row counts,
               find differences if any and highlight them
            3) highlight all columns of the data frame that has additional rows
            """
            attr = 'background-color: {}'.format(color)
            benchmark_data = data[data['source'] == 'benchmark results']
            correctness_data = data[data['source'] == 'correctness results']
            benchmark_rowcount, correctness_rowcount = benchmark_data.shape[0], correctness_data.shape[0]

            minrowcount = min(benchmark_rowcount, correctness_rowcount)

            # splitting benchmark results
            benchmark_data_part_1 = benchmark_data.iloc[:minrowcount]
            benchmark_data_part_2 = benchmark_data.iloc[minrowcount:]
            # splitting correctness results
            correctness_data_part1 = correctness_data.iloc[:minrowcount]
            correctness_data_part2 = correctness_data.iloc[minrowcount:]

            # prepare result style 1 and highlight the differences
            is_equal = benchmark_data_part_1.values == correctness_data_part1.values
            res_benchmark_style = pd.DataFrame(numpy.where(is_equal, '', attr), index=benchmark_data_part_1.index,
                                               columns=benchmark_data_part_1.columns)
            res_correctness_style = pd.DataFrame(numpy.where(is_equal, '', attr), index=correctness_data_part1.index,
                                                 columns=correctness_data_part1.columns)
            result_style_part_1 = pd.concat([res_benchmark_style, res_correctness_style])
            result_style_part_1['source'] = result_style_part_1['source'].apply(lambda x: '')

            # prepare result style 2 and highlight all of them
            result_style_part_2 = pd.concat([benchmark_data_part_2, correctness_data_part2])
            result_style_part_2.loc[:, :] = attr

            result_style = pd.concat([result_style_part_1, result_style_part_2])
            return result_style

        Swarm64Styler = Styler.from_custom_template("resources", "report.tpl")

        return Swarm64Styler(df).apply(highlight_difference, axis=None).render(table_title=table_title)
