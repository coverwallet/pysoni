import pytest

from pysoni.helpers import validate_types


@pytest.fixture
def subject():
    return [4, 8, 15, 16, 23, 42]

def test_no_exception_if_we_do_not_specify_types(subject):
    validate_types(subject)

def test_no_exception_if_expected_types_matches_and_contained_types_is_not_specified(subject):
    validate_types(subject, expected_types=[tuple, list, set])

def test_no_exception_if_contained_types_matches_and_expected_types_is_not_specified(subject):
    validate_types(subject, contained_types=[int, str, set])

def test_no_exception_if_expected_types_and_contained_types_match(subject):
    validate_types(subject, expected_types=[tuple, list], contained_types=[int])

def test_no_exception_if_subject_not_iterable_and_no_contained_types(subject):
    validate_types(4, expected_types=[int, float])

def test_type_error_if_expected_types_does_not_match(subject):
    with pytest.raises(TypeError):
        validate_types(subject, expected_types=[tuple, set])

def test_value_error_if_contained_types_does_not_match(subject):
    with pytest.raises(ValueError):
        validate_types(subject, contained_types=[float, str])

def test_type_error_if_expected_types_and_contained_types_do_not_match(subject):
    with pytest.raises(TypeError):
            validate_types(subject, expected_types=[tuple, set], contained_types=[float, str])
