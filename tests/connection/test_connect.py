import psycopg2


def test_connect_opens_the_connection_correctly(connection, mocker):
    mocker.patch.object(psycopg2, 'connect')
    expected_connection_args = {
      'port': '5432', 'host': 'localhost', 'user': 'cw_test',
      'dbname': 'coverwalletdwh', 'password': '', 'options': None
    }

    connection.connect()

    psycopg2.connect.assert_called_once_with(**expected_connection_args)

def test_connect_sets_is_opened_to_true(connection):
    connection.connect()

    assert connection._is_opened

def test_connect_if_connection_is_already_open_does_not_connect(open_connection):
    open_connection.connect()

    open_connection._connection_handler.connect.assert_not_called()