import pytest
import psycopg2


def test_connection(pysoni_client, mocker):
    mocker.patch.object(psycopg2, 'connect')
    
    pysoni_client.connection()

    psycopg2.connect.assert_called_once_with(
        dbname='coverwalletdwh', user='cw_test', password='',
        host='localhost', port='5432')

def test_invalid_connection(pysoni_invalid_client):

    with pytest.raises(psycopg2.DatabaseError):
        pysoni_invalid_client.connection()

