def test_terminate_closes_non_persistent_connection(open_connection):
    initial_handler = open_connection._connection_handler
    open_connection.is_persistent = False

    open_connection.terminate()

    initial_handler.close.assert_called()

def test_terminate_closes_persistent_connection(open_connection):
    initial_handler = open_connection._connection_handler
    open_connection.is_persistent = True

    open_connection.terminate()

    initial_handler.close.assert_called()

def test_terminate_sets_is_opened_to_false(open_connection):
    open_connection.terminate()

    assert not open_connection._is_opened

def test_terminate_does_nothing_on_closed_connections(connection, mocker):
    connection._connection_handler = mocker.Mock()

    connection.terminate()

    connection._connection_handler.close.assert_not_called()
