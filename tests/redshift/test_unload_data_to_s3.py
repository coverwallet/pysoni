import pytest
from pysoni import Postgre


def test_unload_data_to_s3_with_pysoni_core_api(pysoni_redshift_client, mocker):

    mocker.patch.object(Postgre, 'postgre_statement')

    pysoni_redshift_client.unload_data_to_s3(query='select * from accounts',
                                             s3_path='s3://mybucket/mykey',
                                             copy_options=['csv'],
                                             aws_region='us-east-1')
    expected_statement = "UNLOAD ('select * from accounts') TO 's3://mybucket/mykey' CREDENTIALS 'aws_access_key_id=aws_access_key_id;aws_secret_access_key=aws_access_secret_key' csv REGION AS us-east-1"

    Postgre.postgre_statement.assert_called_once_with(expected_statement)
