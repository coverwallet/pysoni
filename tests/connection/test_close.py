def test_close_closes_the_connection(open_connection, mocker):
    initial_handler = open_connection._connection_handler

    open_connection.close()

    initial_handler.close.assert_called_once()

def test_close_sets_the_handler_to_none(open_connection):
    assert open_connection._connection_handler

    open_connection.close()

    assert open_connection._connection_handler == None