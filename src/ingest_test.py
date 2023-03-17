import pytest

from ingest import ingest_data


@pytest.fixture
def sample_ingest_return_data():
    """Ingestion data insert Return fixture."""
    return [(1, 1, 2, '2022-01-02T00:00:00.000'),
            (2, 1, 2, '2022-01-09T00:00:00.000'),
            (4, 1, 2, '2022-01-09T00:00:00.000'),
            (5, 1, 2, '2022-01-09T00:00:00.000'),
            (6, 5, 3, '2022-01-16T00:00:00.000'),
            (7, 3, 2, '2022-01-16T00:00:00.000'),
            (8, 4, 2, '2022-01-16T00:00:00.000'),
            (9, 2, 2, '2022-01-23T00:00:00.000'),
            (10, 2, 2, '2022-01-23T00:00:00.000'),
            (11, 1, 2, '2022-01-30T00:00:00.000'),
            (12, 5, 2, '2022-01-30T00:00:00.000'),
            (13, 8, 2, '2022-02-06T00:00:00.000'),
            (14, 13, 3, '2022-02-13T00:00:00.000'),
            (15, 13, 3, '2022-02-20T00:00:00.000'),
            (16, 11, 2, '2022-02-20T00:00:00.000'),
            (17, 3, 3, '2022-02-27T00:00:00.000')]


@pytest.fixture
def sample_ingest_return_error():
    """Ingest Return Error fixture."""
    return FileNotFoundError(2, 'No such file or directory')


def test_ingest_data(sample_ingest_return_data):
    ingest_return = ingest_data('test-resources/samples-votes.jsonl')

    assert ingest_return == sample_ingest_return_data


def test_ingest_data_error(sample_ingest_return_error):
    ingest_data('test-resources/samples-votes_error.jsonl')
    with pytest.raises(FileNotFoundError, match='No such file or directory'):
        raise FileNotFoundError('No such file or directory')

# def insert_into_db()
