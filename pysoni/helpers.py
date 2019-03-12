import random


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