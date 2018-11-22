import pytest


def test_with_filename_returns_file_content(pysoni_client):
    filename = 'tests/fixtures/query'

    query = pysoni_client.read_query(filename)

    assert query == 'SELECT * FROM tablename limit 1000'


def test_with_path_and_filename_returns_file_content(pysoni_client):
    filename = 'query'
    path = 'tests/fixtures/'

    query = pysoni_client.read_query(filename, path)

    assert query == 'SELECT * FROM tablename limit 1000'


def test_filename_does_not_exist_raise_file_not_found_error(pysoni_client):
    filename = 'query'
    path = 'test/fixtures/'

    with pytest.raises(FileNotFoundError):
         pysoni_client.read_query(filename, path)
