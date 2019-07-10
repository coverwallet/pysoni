import os
import pytest
import psycopg2
from datetime import datetime
from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv())


def test_successful_update(pysoni_client_persistent_connection_with_envvars):

    pysoni_client_persistent_connection_with_envvars.postgre_statement("CREATE SCHEMA IF NOT EXISTS hr;")
    pysoni_client_persistent_connection_with_envvars.postgre_statement(
        "CREATE TABLE IF NOT EXISTS hr.payments (id INT, name VARCHAR(50), quantity float8);")
    pysoni_client_persistent_connection_with_envvars.postgre_statement("TRUNCATE TABLE hr.payments;")

    insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
    pysoni_client_persistent_connection_with_envvars.execute_batch_inserts(insert_list, "hr.payments")

    update_list = [(2, "Lisa", 40.5), (5, "Frank", 67.3)]
    pysoni_client_persistent_connection_with_envvars.update_table_with_temp_table(
        "hr", "payments", update_list, "id")

    results = pysoni_client_persistent_connection_with_envvars.postgre_to_tuple(
        "SELECT * FROM hr.payments")
    expected_results = [(1, "John", 32.5), (3, "Daniel", 4.12), (2, "Lisa", 40.5),
                        (5, "Frank", 67.3)]

    pysoni_client_persistent_connection_with_envvars.postgre_statement("TRUNCATE TABLE hr.payments;")

    assert expected_results == results


def test_successful_update_with_unordered_columns(pysoni_client_persistent_connection_with_envvars):

    pysoni_client_persistent_connection_with_envvars.postgre_statement("TRUNCATE TABLE hr.payments;")

    insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
    pysoni_client_persistent_connection_with_envvars.execute_batch_inserts(insert_list, "hr.payments")

    update_list = [(2, 40.5, "Lisa"), (5, 67.3, "Frank")]
    pysoni_client_persistent_connection_with_envvars.update_table_with_temp_table(
        "hr", "payments", update_list, "id", columns=['id', 'quantity', 'name'])

    results = pysoni_client_persistent_connection_with_envvars.postgre_to_tuple(
        "SELECT * FROM hr.payments")
    expected_results = [(1, "John", 32.5), (3, "Daniel", 4.12), (2, "Lisa", 40.5),
                        (5, "Frank", 67.3)]

    pysoni_client_persistent_connection_with_envvars.postgre_statement("TRUNCATE TABLE hr.payments;")

    assert expected_results == results


def test_successful_update_with_less_values_than_columns(pysoni_client_persistent_connection_with_envvars):

    pysoni_client_persistent_connection_with_envvars.postgre_statement("TRUNCATE TABLE hr.payments;")

    insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
    pysoni_client_persistent_connection_with_envvars.execute_batch_inserts(insert_list, "hr.payments")

    update_list = [(2, 30.5), (5, 50.2)]
    pysoni_client_persistent_connection_with_envvars.update_table_with_temp_table(
        "hr", "payments", update_list, "id", columns=['id', 'quantity'])

    results = pysoni_client_persistent_connection_with_envvars.postgre_to_tuple(
        "SELECT * FROM hr.payments")
    expected_results = [(1, "John", 32.5), (3, "Daniel", 4.12), (2, None, 30.5),
                        (5, None, 50.2)]

    pysoni_client_persistent_connection_with_envvars.postgre_statement("TRUNCATE TABLE hr.payments;")

    assert expected_results == results


def test_successful_update_with_no_merge_key(pysoni_client_persistent_connection_with_envvars):

    pysoni_client_persistent_connection_with_envvars.postgre_statement("TRUNCATE TABLE hr.payments;")

    insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
    pysoni_client_persistent_connection_with_envvars.execute_batch_inserts(insert_list, "hr.payments")

    update_list = [(4, "John", 61.5), (5, "Lisa", 30.2), (6, "Andrei", 1.86)]
    pysoni_client_persistent_connection_with_envvars.update_table_with_temp_table(
        "hr", "payments", update_list, merge_key=None)

    results = pysoni_client_persistent_connection_with_envvars.postgre_to_tuple(
        "SELECT * FROM hr.payments")
    expected_results = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12),
                        (4, "John", 61.5), (5, "Lisa", 30.2), (6, "Andrei", 1.86)]

    pysoni_client_persistent_connection_with_envvars.postgre_statement("TRUNCATE TABLE hr.payments;")

    assert expected_results == results


def test_successful_update_with_truncation_and_no_merge_key(pysoni_client_persistent_connection_with_envvars):

    pysoni_client_persistent_connection_with_envvars.postgre_statement("TRUNCATE TABLE hr.payments;")

    insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
    pysoni_client_persistent_connection_with_envvars.execute_batch_inserts(insert_list, "hr.payments")

    update_list = [(4, "John", 61.5), (5, "Lisa", 30.2), (6, "Andrei", 1.86)]
    pysoni_client_persistent_connection_with_envvars.update_table_with_temp_table(
        "hr", "payments", update_list, merge_key=None, truncate_table=True)

    results = pysoni_client_persistent_connection_with_envvars.postgre_to_tuple(
        "SELECT * FROM hr.payments")
    expected_results = [(4, "John", 61.5), (5, "Lisa", 30.2), (6, "Andrei", 1.86)]

    pysoni_client_persistent_connection_with_envvars.postgre_statement("TRUNCATE TABLE hr.payments;")

    assert expected_results == results


def test_update_without_persistent_connection(pysoni_client_connection_with_envvars):

    with pytest.raises(ConnectionError):
        insert_list = [(1, "John", 32.5), (2, "Lisa", 29.3), (3, "Daniel", 4.12)]
        pysoni_client_connection_with_envvars.update_table_with_temp_table(
            "hr", "payments", insert_list, "id")
    pysoni_client_connection_with_envvars.postgre_statement("TRUNCATE TABLE hr.payments;")