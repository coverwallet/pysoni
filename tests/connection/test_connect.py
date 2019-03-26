import psycopg2


def test_connect_opens_the_connection_correctly(connection, mocker):
    mocker.patch.object(psycopg2, 'connect')
    expected_connection_args = {
      'port': '5432', 'host': 'localhost', 'user': 'cw_test',
      'dbname': 'coverwalletdwh', 'password': '', 'options': None
    }

    connection.connect()

    psycopg2.connect.assert_called_once_with(**expected_connection_args)
