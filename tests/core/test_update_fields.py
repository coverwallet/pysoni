import pytest

def test_update_fields(pysoni_client, mocker):
    mocker.patch.object(pysoni_client, "connection")
    mocker.patch.object(pysoni_client, "postgre_statement")

    expected_statement = ("UPDATE table SET column='2' "
                          "WHERE column='1'")
                                    

    pysoni_client.update_fields(
        tablename='table',
        column='column',
        values=[['1','2']],
        wait_time=0
    )

    pysoni_client.postgre_statement.assert_called_once_with(
        expected_statement,
        timesleep=0
    )