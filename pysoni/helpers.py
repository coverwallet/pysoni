code import random


def validate_types(subject, expected_types=[], contained_types=[]):
    if expected_types and type(subject) not in expected_types:
        raise TypeError('The type of the subject does not match the expected ones')

    if contained_types:
        element = random.choice(subject)
        if type(element) not in contained_types:
            raise ValueError('The type of the elements inside the subject do not match the expected ones')

def format_sql_string(subject):
    if type(subject) in (list, tuple):
        insert_columns = ','.join(str(subject_object) for subject_object in subject)
    elif type(subject) is str:
        insert_columns = subject
    else:
        raise TypeError('The subject type does not match a sql formatable type.')
    
    return insert_columns
