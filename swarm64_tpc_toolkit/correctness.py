import logging
import os
import pandas as pd

LOG = logging.getLogger()


class CorrectnessCheck:
    def __init__(self, scale_factor, benchmark):
        self.scale_factor = scale_factor
        self.query_output_folder = 'query_results'
        self.correctness_results_folder = os.path.join('correctness_results',
                                                       benchmark, f'sf{self.scale_factor}')

    def get_correctness_filepath(self, query_id):
        filepath = os.path.join(self.correctness_results_folder, f'{query_id}.csv')
        return filepath

    @staticmethod
    def has_differences(first_df, second_df):

        if first_df.empty != second_df.empty:
            return True

        diff = first_df.merge(second_df, indicator='merge', how='outer')
        diff = diff[diff['merge'] != 'both'].drop('merge', axis=1)
        diff_rows_count = diff.shape[0]

        if diff_rows_count > 0:
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
            LOG.debug(f'Query results for {stream_id}-{query_number} not found. Reporting as mismatch.')
            return 'Mismatch'

        if CorrectnessCheck.has_differences(benchmark_result, correctness_result):
            return 'Mismatch'

        return 'OK'
