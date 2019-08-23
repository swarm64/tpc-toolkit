import logging
import os
import pandas as pd

LOG = logging.getLogger()


class CorrectnessCheck:
    def __init__(self, scale_factor, benchmark):
        self.scale_factor = scale_factor
        self.query_output_folder = 'query_results'
        self.correctness_results_folder = os.path.join('correctness_results', benchmark, f'sf{self.scale_factor}')

    def get_correctness_filepath(self, query_id):
        filepath = os.path.join(self.correctness_results_folder, f'{query_id}.csv')
        return filepath

    @staticmethod
    def make_equal_types(first_df, second_df):
        for col in first_df:
            if first_df[col].dtype == 'object':
                second_df[col] = second_df[col].astype(str)
            else:
                second_df[col] = second_df[col].astype(first_df[col].dtype)

        return second_df

    @staticmethod
    def has_differences(first_df, second_df):

        if first_df.empty != second_df.empty:
            return True

        first_df = CorrectnessCheck.make_equal_types(second_df, first_df)

        diff = first_df.merge(second_df, indicator='merge', how='outer')
        diff = diff[diff['merge'] != 'both'].drop('merge', axis=1)
        diff_rows_count = diff.shape[0]

        if diff_rows_count > 0:
            return True
        else:
            return False

    def check_correctness(self, stream_id, query_number):

        LOG.debug(f'Checking Stream={stream_id}, Query={query_number}')
        filepath = os.path.join(self.query_output_folder, f'{stream_id}_{query_number}.csv')
        benchmark_result = correctness_result = pd.DataFrame(columns=['col'])

        try:
            benchmark_result = pd.read_csv(filepath)
        except pd.errors.EmptyDataError:
            LOG.debug(f'{stream_id}_{query_number}.csv empty in benchmark results.')

        try:
            correctness_result = pd.read_csv(self.get_correctness_filepath(query_number))
        except pd.errors.EmptyDataError:
            LOG.debug(f'Query {query_number} is empty in correctness results.')
        except FileNotFoundError:
            LOG.debug(f'File not found!')

        return 'MisMatch' if CorrectnessCheck.has_differences(benchmark_result, correctness_result) else 'OK'

