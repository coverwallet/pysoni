import pytest


def test_redshift_instance(pysoni_redshift_client):
    assert pysoni_redshift_client.aws_access_key_id == 'aws_access_key_id'
    assert pysoni_redshift_client.aws_access_secret_key == 'aws_access_secret_key'
    assert pysoni_redshift_client.aws_creds == f'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key'
