import pytest
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def test_successful_update(pysoni_client_persistent_connection_with_envvars):

    pysoni_client_persistent_connection_with_envvars.postgre_statement("CREATE SCHEMA IF NOT EXISTS hr;")
    pysoni_client_persistent_connection_with_envvars.postgre_statement("DROP TABLE IF EXISTS hr.payments;")
    pysoni_client_persistent_connection_with_envvars.postgre_statement(
        "CREATE TABLE hr.payments (id INT, name VARCHAR(50), quantity float8);")

    insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
    pysoni_client_persistent_connection_with_envvars.execute_batch_inserts(insert_list, "hr.payments")

    update_list = [(2, "Lisa", 40.5), (5, "Frank", 67.3)]
    pysoni_client_persistent_connection_with_envvars.update_table_with_temp_table(
        "payments", update_list, "hr", "id")

    results = pysoni_client_persistent_connection_with_envvars.postgre_to_tuple(
        "SELECT * FROM hr.payments")
    expected_results = [(1, "John", 32.5), (3, "Daniel", 4.12), (2, "Lisa", 40.5),
                        (5, "Frank", 67.3)]
    pysoni_client_persistent_connection_with_envvars.postgre_statement(
        "DROP TABLE hr.payments;")
    pysoni_client_persistent_connection_with_envvars.postgre_statement("DROP SCHEMA hr;")

    assert expected_results == results


def test_successful_update_with_unordered_columns(pysoni_client_persistent_connection_with_envvars):

    pysoni_client_persistent_connection_with_envvars.postgre_statement(
        "CREATE TEMP TABLE payments (id INT, name VARCHAR(50), quantity float8);")

    insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
    pysoni_client_persistent_connection_with_envvars.execute_batch_inserts(insert_list, "payments")

    update_list = [(2, 40.5, "Lisa"), (5, 67.3, "Frank")]
    pysoni_client_persistent_connection_with_envvars.update_table_with_temp_table(
        "payments", update_list, merge_key="id", columns=['id', 'quantity', 'name'])

    results = pysoni_client_persistent_connection_with_envvars.postgre_to_tuple(
        "SELECT * FROM payments")
    expected_results = [(1, "John", 32.5), (3, "Daniel", 4.12), (2, "Lisa", 40.5),
                        (5, "Frank", 67.3)]

    assert expected_results == results


def test_successful_update_with_less_values_than_columns(pysoni_client_persistent_connection_with_envvars):

    pysoni_client_persistent_connection_with_envvars.postgre_statement(
        "CREATE TEMP TABLE payments (id INT, name VARCHAR(50), quantity float8);")

    insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
    pysoni_client_persistent_connection_with_envvars.execute_batch_inserts(insert_list, "payments")

    update_list = [(2, 30.5), (5, 50.2)]
    pysoni_client_persistent_connection_with_envvars.update_table_with_temp_table(
        "payments", update_list, merge_key="id", columns=['id', 'quantity'])

    results = pysoni_client_persistent_connection_with_envvars.postgre_to_tuple(
        "SELECT * FROM payments")
    expected_results = [(1, "John", 32.5), (3, "Daniel", 4.12), (2, None, 30.5),
                        (5, None, 50.2)]

    assert expected_results == results


def test_successful_update_with_no_merge_key(pysoni_client_persistent_connection_with_envvars):

    pysoni_client_persistent_connection_with_envvars.postgre_statement(
        "CREATE TEMP TABLE payments (id INT, name VARCHAR(50), quantity float8);")

    insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
    pysoni_client_persistent_connection_with_envvars.execute_batch_inserts(insert_list, "payments")

    update_list = [(4, "John", 61.5), (5, "Lisa", 30.2), (6, "Andrei", 1.86)]
    pysoni_client_persistent_connection_with_envvars.update_table_with_temp_table(
         "payments", update_list, merge_key=None)

    results = pysoni_client_persistent_connection_with_envvars.postgre_to_tuple(
        "SELECT * FROM payments")
    expected_results = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12),
                        (4, "John", 61.5), (5, "Lisa", 30.2), (6, "Andrei", 1.86)]

    assert expected_results == results


def test_successful_update_with_truncation_and_no_merge_key(pysoni_client_persistent_connection_with_envvars):

    pysoni_client_persistent_connection_with_envvars.postgre_statement(
        "CREATE TEMP TABLE payments (id INT, name VARCHAR(50), quantity float8);")

    insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
    pysoni_client_persistent_connection_with_envvars.execute_batch_inserts(insert_list, "payments")

    update_list = [(4, "John", 61.5), (5, "Lisa", 30.2), (6, "Andrei", 1.86)]
    pysoni_client_persistent_connection_with_envvars.update_table_with_temp_table(
         "payments", update_list, merge_key=None, truncate_table=True)

    results = pysoni_client_persistent_connection_with_envvars.postgre_to_tuple(
        "SELECT * FROM payments")
    expected_results = [(4, "John", 61.5), (5, "Lisa", 30.2), (6, "Andrei", 1.86)]

    assert expected_results == results


def test_update_without_persistent_connection(pysoni_client_connection_with_envvars):

    pysoni_client_connection_with_envvars.postgre_statement(
        "CREATE TEMP TABLE payments (id INT, name VARCHAR(50), quantity float8);")

    with pytest.raises(ConnectionError):
        insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
        pysoni_client_connection_with_envvars.update_table_with_temp_table(
             "payments", insert_list, "id")