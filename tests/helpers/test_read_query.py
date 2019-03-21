import pytest

from pysoni import helpers


def test_with_filename_returns_file_content():
    filename = 'tests/fixtures/query'

    query = helpers.read_query(filename)

    assert query == 'SELECT * FROM tablename limit 1000'


def test_with_path_and_filename_returns_file_content():
    filename = 'query'
    path = 'tests/fixtures/'

    query = helpers.read_query(filename, path)

    assert query == 'SELECT * FROM tablename limit 1000'


def test_filename_does_not_exist_raise_file_not_found_error():
    filename = 'query'
    path = 'path/that/does/not/exist/'

    with pytest.raises(FileNotFoundError):
         helpers.read_query(filename, path)
