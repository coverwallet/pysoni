import unittest

from pysoni.helpers import validate_types


class TestValidateTypes(unittest.TestCase):

    def setUp(self):
        self.subject = [4, 8, 15, 16, 23, 42]

    def test_no_exception_if_we_do_not_specify_types(self):
        validate_types(self.subject)

    def test_no_exception_if_expected_types_matches_and_contained_types_is_not_specified(self):
        validate_types(self.subject, expected_types=[tuple, list, set])

    def test_no_exception_if_contained_types_matches_and_expected_types_is_not_specified(self):
        validate_types(self.subject, contained_types=[int, str, set])

    def test_no_exception_if_expected_types_and_contained_types_match(self):
        validate_types(self.subject, expected_types=[tuple, list], contained_types=[int])

    def test_no_exception_if_subject_not_iterable_and_no_contained_types(self):
        validate_types(4, expected_types=[int, float])

    def test_type_error_if_expected_types_does_not_match(self):
        with self.assertRaises(TypeError):
            validate_types(self.subject, expected_types=[tuple, set])

    def test_value_error_if_contained_types_does_not_match(self):
        with self.assertRaises(ValueError):
            validate_types(self.subject, contained_types=[float, str])

    def test_type_error_if_expected_types_and_contained_types_do_not_match(self):
        with self.assertRaises(TypeError):
            validate_types(self.subject, expected_types=[tuple, set], contained_types=[float, str])