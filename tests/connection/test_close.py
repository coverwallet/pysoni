def test_close_closes_the_connection(open_connection, mocker):
    initial_handler = open_connection._connection_handler

    open_connection.close()

    initial_handler.close.assert_called_once()

def test_close_sets_the_handler_to_none(open_connection):
    assert open_connection._connection_handler

    open_connection.close()

    assert open_connection._connection_handler == None

def test_close_sets_is_opened_to_false(open_connection):
    open_connection.close()

    assert not open_connection._is_opened

def test_close_if_the_connection_is_not_opened_does_not_close(connection, mocker):
    connection._connection_handler = mocker.Mock()
    connection._connection_handler.closed = 1

    connection.close()

    connection._connection_handler.close.assert_not_called()

def test_close_if_connection_is_persistent_does_not_close(open_connection):
    open_connection.is_persistent = True

    open_connection.close()

    assert open_connection._is_opened
    assert open_connection._connection_handler