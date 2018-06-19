import random

def validate_types(subject, expected_types=[], contained_types=[]):
    if expected_types and type(subject) not in expected_types:
        raise TypeError('The type of the subject does not match the expected ones')

    element = random.choice(subject)
    if contained_types and type(element) not in contained_types:
        raise ValueError('The type of the elements inside the subject do not match the expected ones')
