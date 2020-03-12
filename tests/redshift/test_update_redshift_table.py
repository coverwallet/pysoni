import pytest
from pysoni import Postgre


def test_update_redshift_table_raise_connection_error(pysoni_redshift_client_not_persistent_connection):

    with pytest.raises(ConnectionError):
        pysoni_redshift_client_not_persistent_connection.update_table_from_s3(
            s3_path='s3://mybucket/mykey', table_name='test_table', columns=['column1', 'column2'],
            copy_options=["IGNOREHEADER 1", "BLANKSASNULL", "EMPTYASNULL", "IGNOREBLANKLINES", "CSV DELIMITER '|'",
                          "TIMEFORMAT 'auto'", "TRUNCATECOLUMNS"], tmp_table_name='test_table_name',
            table_schema='public', delete_statement='TRUNCATE TABLE test_table',
            posthook_statement='GRANT SELECT ON public.test_table TO GROUP test_users')


def test_update_redshift_table_with_pysoni_core_api(pysoni_redshift_client, mocker):

    mocker.patch.object(Postgre, 'postgre_statement')
    pysoni_redshift_client.update_table_from_s3(
        s3_path='s3://mybucket/mykey', table_name='test_table', columns=['column1', 'column2'],
        copy_options=["IGNOREHEADER 1", "BLANKSASNULL", "EMPTYASNULL", "IGNOREBLANKLINES", "CSV DELIMITER '|'",
                      "TIMEFORMAT 'auto'", "TRUNCATECOLUMNS"], tmp_table_name='test_temp_table_name',
        table_schema='public', delete_statement='TRUNCATE TABLE test_table',
        posthook_statement='GRANT SELECT ON public.test_table TO GROUP test_users')
    expected_statement = ("CREATE TEMP TABLE test_temp_table_name AS (SELECT column1,column2 FROM public.test_table LIMIT 0);COPY test_temp_table_name (column1,column2) FROM 's3://mybucket/mykey' with credentials 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL IGNOREBLANKLINES CSV DELIMITER '|' TIMEFORMAT 'auto' TRUNCATECOLUMNS;TRUNCATE TABLE test_table;INSERT INTO public.test_table(column1,column2) (SELECT column1,column2 FROM test_temp_table_name);GRANT SELECT ON public.test_table TO GROUP test_users")
    Postgre.postgre_statement.assert_called_once_with(expected_statement)


def test_update_redshift_table_with_pysoni_core_api_without_delete_statement(pysoni_redshift_client, mocker):

    mocker.patch.object(Postgre, 'postgre_statement')
    pysoni_redshift_client.update_table_from_s3(
        s3_path='s3://mybucket/mykey', table_name='test_table', columns=['column1', 'column2'],
        copy_options=["IGNOREHEADER 1", "BLANKSASNULL", "EMPTYASNULL", "IGNOREBLANKLINES", "CSV DELIMITER '|'",
                      "TIMEFORMAT 'auto'", "TRUNCATECOLUMNS"], tmp_table_name='test_temp_table_name',
        table_schema='public', posthook_statement='GRANT SELECT ON public.test_table TO GROUP test_users')
    expected_statement = ("CREATE TEMP TABLE test_temp_table_name AS (SELECT column1,column2 FROM public.test_table LIMIT 0);COPY test_temp_table_name (column1,column2) FROM 's3://mybucket/mykey' with credentials 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL IGNOREBLANKLINES CSV DELIMITER '|' TIMEFORMAT 'auto' TRUNCATECOLUMNS;INSERT INTO public.test_table(column1,column2) (SELECT column1,column2 FROM test_temp_table_name);GRANT SELECT ON public.test_table TO GROUP test_users")
    Postgre.postgre_statement.assert_called_once_with(expected_statement)


def test_update_redshift_table_with_pysoni_core_api_without_posthook_statement(pysoni_redshift_client, mocker):

    mocker.patch.object(Postgre, 'postgre_statement')
    pysoni_redshift_client.update_table_from_s3(
        s3_path='s3://mybucket/mykey', table_name='test_table', columns=['column1', 'column2'],
        copy_options=["IGNOREHEADER 1", "BLANKSASNULL", "EMPTYASNULL", "IGNOREBLANKLINES", "CSV DELIMITER '|'",
                      "TIMEFORMAT 'auto'", "TRUNCATECOLUMNS"], tmp_table_name='test_temp_table_name',
        table_schema='public')
    expected_statement = ("CREATE TEMP TABLE test_temp_table_name AS (SELECT column1,column2 FROM public.test_table LIMIT 0);COPY test_temp_table_name (column1,column2) FROM 's3://mybucket/mykey' with credentials 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL IGNOREBLANKLINES CSV DELIMITER '|' TIMEFORMAT 'auto' TRUNCATECOLUMNS;INSERT INTO public.test_table(column1,column2) (SELECT column1,column2 FROM test_temp_table_name)")
    Postgre.postgre_statement.assert_called_with(expected_statement)


def test_update_redshift_table_with_pysoni_core_api_without_columns(pysoni_redshift_client, mocker):

    mocker.patch.object(Postgre, 'postgre_statement')
    pysoni_redshift_client.update_table_from_s3(
        s3_path='s3://mybucket/mykey', table_name='test_table',
        copy_options=["IGNOREHEADER 1", "BLANKSASNULL", "EMPTYASNULL", "IGNOREBLANKLINES", "CSV DELIMITER '|'",
                      "TIMEFORMAT 'auto'", "TRUNCATECOLUMNS"], tmp_table_name='test_temp_table_name',
        table_schema='public', posthook_statement='GRANT SELECT ON public.test_table TO GROUP test_users')
    expected_statement = ("CREATE TEMP TABLE test_temp_table_name AS (SELECT * FROM public.test_table LIMIT 0);COPY test_temp_table_name  FROM 's3://mybucket/mykey' with credentials 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL IGNOREBLANKLINES CSV DELIMITER '|' TIMEFORMAT 'auto' TRUNCATECOLUMNS;INSERT INTO public.test_table (SELECT * FROM test_temp_table_name);GRANT SELECT ON public.test_table TO GROUP test_users")
    Postgre.postgre_statement.assert_called_with(expected_statement)


def test_update_redshift_table_with_pysoni_core_api_without_default_statements(pysoni_redshift_client, mocker):

    mocker.patch.object(Postgre, 'postgre_statement')
    pysoni_redshift_client.update_table_from_s3(
        s3_path='s3://mybucket/mykey', table_name='test_table',
        copy_options=["IGNOREHEADER 1", "BLANKSASNULL", "EMPTYASNULL", "IGNOREBLANKLINES", "CSV DELIMITER '|'",
                      "TIMEFORMAT 'auto'", "TRUNCATECOLUMNS"], tmp_table_name='test_temp_table_name')

    excepted_statement = ("CREATE TEMP TABLE test_temp_table_name AS (SELECT * FROM public.test_table LIMIT 0);COPY test_temp_table_name  FROM 's3://mybucket/mykey' with credentials 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL IGNOREBLANKLINES CSV DELIMITER '|' TIMEFORMAT 'auto' TRUNCATECOLUMNS;INSERT INTO public.test_table (SELECT * FROM test_temp_table_name)")
    Postgre.postgre_statement.assert_called_with(excepted_statement)
