def test_close_closes_the_connection(open_connection, mocker):
    initial_handler = open_connection._connection_handler

    open_connection.close()

    initial_handler.close.assert_called_once()

def test_close_sets_is_open_to_false(open_connection):
    open_connection.close()

    assert open_connection.is_open == False

def test_close_does_nothing_with_a_closed_connection(open_connection):
    open_connection.is_open = False

    open_connection.close()

    open_connection._connection_handler.close.assert_not_called()

def test_close_sets_the_handler_to_none(open_connection):
    assert open_connection._connection_handler

    open_connection.close()

    assert open_connection._connection_handler == None