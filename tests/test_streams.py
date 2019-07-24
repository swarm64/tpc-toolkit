
import pytest
import pandas as pd

from swarm64_tpc_toolkit import streams


@pytest.fixture
def args():
    class DefaultArgs:
        config = None
        timeout = 0
        dsn = 'postgresql://noone@nowhere:4321/nothing'
        streams = 0
        netdata_url = None
        benchmark = 'tpch'
        stream_offset = 1
        output = 'csv'

    return DefaultArgs


def test_make_config_no_config_file(mocker, args):
    yaml_mock = mocker.patch('yaml.load')

    obj = streams.Streams(args)

    yaml_mock.assert_not_called()
    assert 'timeout' not in obj.config


def test_make_config_file_present(mocker, args):
    mocker.patch('builtins.open', mocker.mock_open())
    yaml_mock = mocker.patch('yaml.load')

    args.config = 'foo.yaml'
    obj = streams.Streams(args)

    yaml_mock.assert_called_once()
    assert 'timeout' not in obj.config


def test_make_config_override_timeout(mocker, args):
    mocker.patch('builtins.open', mocker.mock_open())
    mocker.patch('yaml.load', return_value={'timeout': 100})

    args.timeout = 101
    obj = streams.Streams(args)

    assert obj.config['timeout'] == 101


def test_read_sql_file(mocker, args):
    open_patched = mocker.patch('builtins.open', mocker.mock_open(read_data='SELECT 1'))

    result = streams.Streams(args).read_sql_file(1024)
    open_patched.assert_called_with(f'queries/{args.benchmark}/1024.sql', 'r')
    assert result == 'SELECT 1'


def test_apply_sql_modifications():
    test_sql = 'SELECT A, B, C FROM foo WHERE bar = 1'
    test_modifications = (('A', 'AA'), ('bar', 'bor'))

    sql = streams.Streams.apply_sql_modifications(test_sql, test_modifications)
    assert sql == 'SELECT AA, B, C FROM foo WHERE bor = 1'


def test_make_run_args(args):
    run_args = streams.Streams(args)._make_run_args()
    assert run_args == ((0,),)

    args.streams = 3
    run_args = streams.Streams(args)._make_run_args()
    assert run_args == ((1,), (2,), (3,))


def test_get_stream_sequence(mocker, args):
    open_patched = mocker.patch('builtins.open', mocker.mock_open(read_data=''))
    mocker.patch('yaml.load', return_value=[
        [0, 1, 2],
        [1, 2, 0],
        [2, 1, 0],
        [1, 0, 2]
    ])

    result = streams.Streams(args).get_stream_sequence(2)
    open_patched.assert_called_with(f'queries/{args.benchmark}/streams.yaml', 'r')
    assert result == [2, 1, 0]


def test_run_single_stream(mocker, args):
    pool_mock = mocker.patch('multiprocessing.pool.Pool', autospec=True)
    pool_mock_obj = pool_mock.return_value.__enter__.return_value

    s = streams.Streams(args)
    s.run_streams()

    stream_ids = ((0,),)
    pool_mock.assert_called_once()
    pool_mock_obj.starmap.assert_called_once_with(s._run_stream, stream_ids)


def test_run_multiple_streams(mocker, args):
    pool_mock = mocker.patch('multiprocessing.pool.Pool', autospec=True)
    pool_mock_obj = pool_mock.return_value.__enter__.return_value

    args.streams = 3
    s = streams.Streams(args)
    s.run_streams()

    stream_ids = ((1,), (2,), (3,))
    pool_mock.assert_called_once()
    pool_mock_obj.starmap.assert_called_once_with(s._run_stream, stream_ids)


def test_run_stream(mocker, args):
    psycopg2_connect = mocker.patch('psycopg2.connect')
    mock_conn = psycopg2_connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    test_sequence = tuple([1, 2001, -3, 4])
    test_sql = 'SELECT 1'
    test_stream_id = 42

    s = streams.Streams(args)
    s.config['ignore'] = [4]
    executed_test_sequence = tuple([1, 2001, -3])

    mocker.patch.object(s, 'get_stream_sequence', return_value=test_sequence, autospec=True)
    read_sql_file_mock = mocker.patch.object(s, 'read_sql_file', return_value=test_sql)

    result = s._run_stream(test_stream_id)

    read_sql_file_mock.assert_has_calls([mocker.call(query_id) for query_id in executed_test_sequence])
    mock_cursor.execute.assert_has_calls([mocker.call(test_sql)] * len(executed_test_sequence))

    assert len(result) == 1
    assert list(result.keys())[0] == test_stream_id
    assert len(result[test_stream_id]) == len(executed_test_sequence)


def test_run(mocker, args):
    s = streams.Streams(args)
    mocker.patch.object(s, '_print_results', autospec=True)
    db_mock = mocker.patch.object(s, 'db', autospec=True)
    run_streams_mock = mocker.patch.object(s, 'run_streams')
    s.run()

    assert db_mock.reset_config.call_count == 2
    db_mock.apply_config.assert_called_once_with({})
    run_streams_mock.assert_called_once()


def test_run_keyboard_interrupt(mocker, args):
    s = streams.Streams(args)
    db_mock = mocker.patch.object(s, 'db', autospec=True)
    run_streams_mock = mocker.patch.object(s, 'run_streams', side_effect=KeyboardInterrupt('Ctrl-C!'))
    s.run()

    assert db_mock.reset_config.call_count == 2
    db_mock.apply_config.assert_called_once_with({})
    run_streams_mock.assert_called_once()

def test_sort_output():
    data_in =(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    index_in = (5, 1, '3a', 3, 11, 20, '3b', 4, 21, 2)

    df = pd.DataFrame(data_in, index=index_in)
    df = streams.Streams.sort_df(df)

    data_out = (2, 10, 4, 3, 7, 8, 1, 5, 6, 9)
    index_out = (1, 2, 3, '3a', '3b', 4, 5, 11, 20, 21)

    assert list(df[0].index) == index_out
    assert list(df[0]) == data_out
