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

    def has_differences(self, truth, result):
        self.truth = truth
        self.result = result

        for truth_idx, truth_row in truth.iterrows():
            for result_idx, result_row in self.result.iterrows():
                row_equal = True
                for column in truth:
                    if truth[column].dtype == 'float64':
                        if not numpy.isclose(truth_row[column], result_row[column], rtol=1e-12, atol=0.01):
                            row_equal = False
                    else:
                        if truth_row[column] != result_row[column]:
                            row_equal = False
                if row_equal:
                    self.truth.drop(index=truth_idx, inplace=True)
                    self.result.drop(index=result_idx, inplace=True)
                    break
            
        return (not self.truth.empty) or (not self.result.empty)

    def check_correctness(self, stream_id, query_number):

        LOG.debug(f'Checking Stream={stream_id}, Query={query_number}')
        correctness_path = self.get_correctness_filepath(query_number)
        benchmark_path = os.path.join(self.query_output_folder, f'{stream_id}_{query_number}.csv')

        # Reading truth
        try:
            truth = pd.read_csv(correctness_path)
        except pd.errors.EmptyDataError:
            LOG.debug(f'Query {query_number} is empty in correctness results.')
            truth = pd.DataFrame(columns=['col'])
        except FileNotFoundError:
            LOG.debug(f'Correctness results for {query_number} not found. Skipping correctness checking.')
            return 'OK'

        # Reading Benchmark results
        try:
            result = pd.read_csv(benchmark_path)
        except pd.errors.EmptyDataError:
            LOG.debug(f'{stream_id}_{query_number}.csv empty in benchmark results.')
            result = pd.DataFrame(columns=['col'])
        except FileNotFoundError:
            msg = f'Query results for {stream_id}-{query_number} not found. Reporting as mismatch.'
            LOG.debug(msg)
            self.html += f'<p>{msg}</p>'
            return 'Mismatch'

        # order the columns so that results are easier to compare
        truth = truth.reindex(sorted(truth.columns), axis=1)
        result = result.reindex(sorted(result.columns), axis=1)

        # check that we have the same columns
        if len(numpy.setdiff1d(truth.columns.values, result.columns.values)) > 0:
            msg = f'Query {stream_id}-{query_number} has mismatching columns.'
            LOG.debug(msg)
            self.html += f'<p>{msg}</p>'
            return 'Mismatch'

        if self.has_differences(truth, result):
            self.html += Correctness.to_html(self.truth,
                                             table_title=f'Truth mismatch in StreamId={stream_id}, Query={query_number}')
            self.html += Correctness.to_html(self.result,
                                             table_title=f'Result mismatch in StreamId={stream_id}, Query={query_number}')

            return 'Mismatch'

        return 'OK'

    @staticmethod
    def to_html(df, table_title):
        Swarm64Styler = Styler.from_custom_template("resources", "report.tpl")

        return Swarm64Styler(df).render(table_title=table_title)
