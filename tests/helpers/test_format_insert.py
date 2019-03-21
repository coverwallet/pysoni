import pytest

from pysoni.helpers import format_insert


def test_when_data_are_tuples_returns_it_unchanged():
    data_to_insert = [(1,), (3.14,), ('test_str',), (42,)]

    assert format_insert(data_to_insert) == data_to_insert

def test_when_data_are_lists_returns_list_of_tuples():
    data_to_insert = [[1], [3.14], ['test_str'], [42]]
    expected_result = [(1,), (3.14,), ('test_str',), (42,)]

    assert format_insert(data_to_insert) == expected_result

def test_when_data_is_float_int_or_str_returns_list_of_tuples():
    data_to_insert = [1, 3.14, 'test_str', 42]
    expected_result = [(1,), (3.14,), ('test_str',), (42,)]

    assert format_insert(data_to_insert) == expected_result