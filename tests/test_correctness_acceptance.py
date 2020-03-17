
import pytest
import pandas

from io import StringIO

from swarm64_tpc_toolkit.correctness import Correctness


# Acceptance test for correctness checking

CSV_BASE = '''cnt,state,value
20,HI,1.0
45,RI,100000000000000000.0
59,CT,123
914,IA,9999999999.131413000000000000000000000000001
101,NH,451
120,,
988,IL,0.5
290,NM,0.000000000000000000000000000000000001
29,DE,0.00000001
986,,42
,MO,
1055,KS,
2420,TX,1024
-1,FOO,9999999999.139413000000000000000000000000001'''

CSV_ERRORS_IDX = [3, 4, 8] # after sorting
CSV_ERRORS = '''cnt,state,value
20,HI,1.0
45,RI,
101,NH,451
120,,
59,CT,123
988,IL,0.5
,NM,0.000000000000000000000000000000000001
914,IA,9999999999.131413000000000000000000000000001
29,,0.00000001
986,,42
,MO,
1055,KS,
2420,TX,1024
-1,FOO,9999999999.139413000000000000000000000000001'''

CSV_ERRORS_PRECISION_IDX = [9]
CSV_ERRORS_PRECISION = '''cnt,state,value
20,HI,1.0
45,RI,100000000000000000.0
59,CT,123
914,IA,9999999999.149413000000000000000000000000001
101,NH,451
120,,
988,IL,0.5
290,NM,0.000000000000000000000000000000000001
29,DE,0.00000001
986,,42
,MO,
1055,KS,
2420,TX,1024
-1,FOO,9999999999.139413000000000000000000000000001'''

CSV_WITHIN_PRECISION = '''cnt,state,value
20,HI,1.0
45,RI,100000000000000000.0
59,CT,123
914,IA,9999999999.131413000000000000000000000000001
101,NH,451
120,,
988,IL,0.5
290,NM,0.000000000000000000000000000000000001
29,DE,0.00000001
986,,42
,MO,
1055,KS,
2420,TX,1024
-1,FOO,9999999999.141413000000000000000000000000001'''


ROWS_TO_DROP = [6, 8]
COLUMNS_TO_DROP = ['state']


@pytest.fixture()
def correctness():
    benchmark = 'tpch'
    scale_factor = 1000

    return Correctness(scale_factor, benchmark)


def get_dataframe(source):
    source_io = StringIO(source)
    return pandas.read_csv(source_io, sep=',')


def test_correctness_truth_empty(correctness):
    truth = pandas.DataFrame()
    result = get_dataframe(CSV_BASE)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(result.index)


def test_correctness_result_empty(correctness):
    truth = get_dataframe(CSV_BASE)
    result = pandas.DataFrame()

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(truth.index)


def test_correctness_full_equal(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_full_equal_within_precision(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_WITHIN_PRECISION)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_full_equal_randomized_rows(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE).sample(frac=1, random_state=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_full_equal_randomized_columns(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE).sample(frac=1, random_state=1, axis=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_full_equal_randomized_full(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE) \
        .sample(frac=1, random_state=1) \
        .sample(frac=1, random_state=1, axis=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_not_equal_row_missing_truth(correctness):
    truth = get_dataframe(CSV_BASE).drop(ROWS_TO_DROP)
    result = get_dataframe(CSV_BASE)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(truth.index)


def test_correctness_not_equal_row_missing_result(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE).drop(ROWS_TO_DROP)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(truth.index)


def test_correctness_not_equal_column_missing_truth(correctness):
    truth = get_dataframe(CSV_BASE).drop(COLUMNS_TO_DROP, axis=1)
    result = get_dataframe(CSV_BASE)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(truth.index)


def test_correctness_not_equal_column_missing_result(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE).drop(COLUMNS_TO_DROP, axis=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(truth.index)


def test_correctness_not_equal(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_ERRORS)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == CSV_ERRORS_IDX


def test_correctness_not_equal_randomized_rows(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_ERRORS).sample(frac=1, random_state=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == CSV_ERRORS_IDX


def test_correctness_not_equal_randomized_columns(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_ERRORS).sample(frac=1, random_state=1, axis=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == CSV_ERRORS_IDX


def test_correctness_not_equal_randomized_full(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_ERRORS) \
        .sample(frac=1, random_state=1) \
        .sample(frac=1, random_state=1, axis=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == CSV_ERRORS_IDX


def test_correctness_not_equal_precision(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_ERRORS_PRECISION)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == CSV_ERRORS_PRECISION_IDX


def test_value_swap_rows(correctness):
    truth = pandas.DataFrame([[1, 2], [4, 5]])
    result = pandas.DataFrame([[2, 1], [4, 5]])

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == [0]


def test_value_swap_columns(correctness):
    truth = pandas.DataFrame([[1, 4], [2, 5]])
    result = pandas.DataFrame([[2, 4], [1, 5]])

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == [0, 1]
