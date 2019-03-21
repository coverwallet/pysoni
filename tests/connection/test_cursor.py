def test_cursor_triggers_cursor_in_the_connection(open_connection):
    open_connection.cursor()

    open_connection._connection_handler.cursor.assert_called_once()

def test_cursor_returns_a_cursor_in_the_handler(open_connection, mocker):
    cursor_mock = mocker.Mock()
    open_connection._connection_handler.cursor.return_value = cursor_mock

    assert open_connection.cursor() == cursor_mock