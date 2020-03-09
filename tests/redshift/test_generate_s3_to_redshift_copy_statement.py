import pytest


def test_generate_s3_to_redshift_copy_statement(pysoni_redshift_client):

    copy_statement = pysoni_redshift_client.generate_s3_to_redshift_copy_statement(
        s3_path='s3://mybucket/mykey', table_name='test_table', columns=['column1', 'column2'],
        copy_options=["IGNOREHEADER 1", "BLANKSASNULL", "EMPTYASNULL", "IGNOREBLANKLINES", "CSV DELIMITER '|'",
                      "TIMEFORMAT 'auto'", "TRUNCATECOLUMNS"],
        table_schema='public')

    assert copy_statement == (
        f"COPY public.test_table (column1,column2) "
        f"FROM 's3://mybucket/mykey' "
        f"with credentials 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' "
        f"IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL IGNOREBLANKLINES CSV DELIMITER '|' TIMEFORMAT 'auto' TRUNCATECOLUMNS")


def test_generate_s3_to_redshift_copy_statement_without_columns_argument(pysoni_redshift_client):

    copy_statement = pysoni_redshift_client.generate_s3_to_redshift_copy_statement(
        s3_path='s3://mybucket/mykey', table_name='test_table',
        copy_options=["IGNOREHEADER 1", "BLANKSASNULL", "EMPTYASNULL", "IGNOREBLANKLINES", "CSV DELIMITER '|'",
                      "TIMEFORMAT 'auto'", "TRUNCATECOLUMNS"],
        table_schema='public')

    assert copy_statement == (
        f"COPY public.test_table  "
        f"FROM 's3://mybucket/mykey' "
        f"with credentials 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' "
        f"IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL IGNOREBLANKLINES CSV DELIMITER '|' TIMEFORMAT 'auto' TRUNCATECOLUMNS")


def test_generate_s3_to_redshift_copy_statement_without_schema_argument(pysoni_redshift_client):

    copy_statement = pysoni_redshift_client.generate_s3_to_redshift_copy_statement(
        s3_path='s3://mybucket/mykey', table_name='test_table', columns=['column1', 'column2'],
        copy_options=["IGNOREHEADER 1", "BLANKSASNULL", "EMPTYASNULL", "IGNOREBLANKLINES", "CSV DELIMITER '|'",
                      "TIMEFORMAT 'auto'", "TRUNCATECOLUMNS"])

    assert copy_statement == (
        f"COPY test_table (column1,column2) "
        f"FROM 's3://mybucket/mykey' "
        f"with credentials 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' "
        f"IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL IGNOREBLANKLINES CSV DELIMITER '|' TIMEFORMAT 'auto' TRUNCATECOLUMNS")


def test_generate_s3_to_redshift_copy_statement_without_copy_options_argument(pysoni_redshift_client):

    copy_statement = pysoni_redshift_client.generate_s3_to_redshift_copy_statement(
        s3_path='s3://mybucket/mykey', table_name='test_table', columns=['column1', 'column2'],
        table_schema='public')

    assert copy_statement == (
        f"COPY public.test_table (column1,column2) "
        f"FROM 's3://mybucket/mykey' "
        f"with credentials 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' ")


def test_generate_s3_to_redshift_copy_statement_without_default_arguments(pysoni_redshift_client):

    copy_statement = pysoni_redshift_client.generate_s3_to_redshift_copy_statement(
        s3_path='s3://mybucket/mykey', table_name='test_table')

    assert copy_statement == (
        f"COPY test_table  "
        f"FROM 's3://mybucket/mykey' "
        f"with credentials 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' ")
