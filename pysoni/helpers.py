import random


def validate_types(subject, expected_types=[], contained_types=[]):
    """Validates the type of an object or its elements if it's an iterable

    It will raise a TypeError exception if the type of the subject don't match
    any of the ones specified in expected_types

    It will raise a ValueError exception if the type of an element of the
    subject (taken at random) doesn't match any of the types specified in
    contained_types

    Arguments
    ---------
    subject : any
        Element that we want to validate
    expected_types : iterable of types
        Types accepted for subject
    contained_types : iterable of types
        Types accepted for the elements of subject (if it's an iterable)
    """
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

def format_insert(data_to_insert):
    """Convert the python object into an admissible PostgreSQL input

    Arguments
    ---------
    data_to_insert : list, tuple
        Iterable containing the values that we want to insert in the DB
    """
    data_type = type(random.choice(data_to_insert))

    if data_type is list:
        return [tuple(value) for value in data_to_insert]
    elif data_type is tuple:
        return data_to_insert
    elif data_type in (str, int, float):
        return [tuple([value]) for value in data_to_insert]

def read_query(name, path=None):
    """Extract a query from a .sql file and return it as a string

    Arguments
    ---------
    name : string
        Name of the file containing the query
    path : string
        Path where the file is located. It has to be either absolute or relative to the project root
    """
    if path:
        filename = f"{path}{name}.sql"
    else:
        filename = f"{name}.sql"

    with open(filename, 'r') as query:
        return query.read()