import pytest


def test_generate_unload_statement(pysoni_redshift_client):
    excepted_statement = "UNLOAD ('select * from account where city=''malaga''') TO 's3://mybucket/mykey' CREDENTIALS 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' HEADER DELIMITER '|' CSV REGION AS us-east-1"

    statement = pysoni_redshift_client.generate_redshift_to_s3_unload_statement(
        query="select * from account where city=''malaga''", s3_path='s3://mybucket/mykey',
        copy_options=["HEADER", "DELIMITER '|'", "CSV"],
        aws_region='us-east-1')

    assert excepted_statement == statement


def test_generate_unload_statement_without_region(pysoni_redshift_client):
    expected_statement = "UNLOAD ('select * from account where city=''malaga''') TO 's3://mybucket/mykey' CREDENTIALS 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' HEADER DELIMITER '|' CSV "

    statement = pysoni_redshift_client.generate_redshift_to_s3_unload_statement(
        query="select * from account where city=''malaga''", s3_path='s3://mybucket/mykey',
        copy_options=["HEADER", "DELIMITER '|'", "CSV"])

    assert expected_statement == statement
