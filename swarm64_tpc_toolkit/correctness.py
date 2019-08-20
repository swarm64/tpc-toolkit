import glob
import logging
import os
import pandas as pd

LOG = logging.getLogger()


class CorrectnessCheck:
    def __init__(self, scale_factor, benchmark, query_results):
        self.scale_factor = scale_factor
        self.query_output_folder = 'query_results'
        self.correctness_results_folder = os.path.join('correctness_results', benchmark)
        self.query_results = query_results

    def get_correctness_filepath(self, query_id):
        filepath = f'{self.correctness_results_folder}/sf{self.scale_factor}_{query_id}.csv'
        return filepath

    def get_queries_results(self):
        for stream_id, timings in self.query_results.iterrows():
            for query_id, timing in timings:
                if timing.status.name == 'OK':
                    yield stream_id, query_id

    @staticmethod
    def from_reference_result_to_df(reference, query_number):
        try:
            reference_result = reference[str(query_number)]['result']

            if reference_result is None or reference[str(query_number)]['state'] != 'ok':
                LOG.info(f'Reference does not exist for Query {query_number}. Skipping...')
                return None

            reference_result_df = pd.DataFrame.from_records(reference_result['rows'],
                                                            columns=reference_result['keys'])

            return reference_result_df

        except KeyError:
            LOG.info(f'Reference does not exist for Query {query_number}. Skipping...')

    @staticmethod
    def make_equal_types(first_df, second_df):
        for col in first_df:
            if first_df[col].dtype == 'object':
                second_df[col] = second_df[col].astype(str)
            else:
                second_df[col] = second_df[col].astype(first_df[col].dtype)

        return second_df

    @staticmethod
    def find_differences(first_df, second_df):

        first_df = CorrectnessCheck.make_equal_types(second_df, first_df)

        diff = first_df.merge(second_df, indicator='merge', how='outer')
        diff = diff[diff['merge'] != 'both'].drop('merge', axis=1)
        diff_rows_count = diff.shape[0]

        if diff_rows_count > 0:
            LOG.warning('Mismatch')
            LOG.warning(diff)
        else:
            LOG.info('Match')

    def check_query_correctness(self):

        for stream_id, query_number in self.get_queries_results():
            LOG.info(f'Checking Stream={stream_id}, Query={query_number}')
            filepath = os.path.join(self.query_output_folder, f'{stream_id}_{query_number}.csv')

            try:
                benchmark_result = pd.read_csv(filepath)
            except pd.errors.EmptyDataError:
                LOG.info(f'{stream_id}_{query_number}.csv empty.')
                benchmark_result = None

            try:
                correctness_results = pd.read_csv(self.get_correctness_filepath(query_number))
            except pd.errors.EmptyDataError:
                LOG.info(f'Query {query_number} is empty in correctness results.')
                correctness_results = None

            CorrectnessCheck.find_differences(benchmark_result, correctness_results)

