
import pytest


from swarm64_tpc_toolkit import stats


@pytest.fixture
def stats_fixture():
    netdata_url = 'http://fake-netdata:19999'
    disk = 'some_disk'
    return stats.Stats(netdata_url, disk)


def test_make_columns():
    metrics = ['foo', 'bar']

    columns_expected = [*stats.BASE_COLUMNS]
    columns_expected.extend([f'{metric}_bar' for metric in stats.STATS_METRICS])
    columns_expected.extend([f'{metric}_foo' for metric in stats.STATS_METRICS])

    columns = stats.Stats.make_columns(metrics)
    assert sorted(columns) == sorted(columns_expected)


# def query_netdata(self, start, end):
def test_query_netdata(mocker, stats_fixture):
    start = 123
    end = 456

    response_value = 'some fancy response'

    def get_return_json():
        return response_value

    requests_get_mock = mocker.patch('requests.get')
    requests_get_mock.return_value.json = get_return_json

    netdata_data = stats_fixture.query_netdata(start, end)

    for idx, chart_key in enumerate(stats_fixture.charts.keys()):
        _, _, kwargs = requests_get_mock.mock_calls[idx]
        assert chart_key == kwargs['params']['chart']
        assert chart_key in netdata_data
        assert netdata_data[chart_key] == response_value


def test_transform(stats_fixture):
    data = {chart_id: {
        'labels': ['foo', 'bar'],
        'data': [[1, 2]],
    } for chart_id in stats_fixture.chart_ids}

    data = stats_fixture.transform(data)

    header_expected = []
    for chart_id in stats_fixture.chart_ids:
        header_expected.append(chart_id + '.foo')
        header_expected.append(chart_id + '.bar')

    assert data[0] == header_expected
    assert data[1] == [1, 2] * 4
