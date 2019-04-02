def test_commit_triggers_commit_in_the_connection(open_connection):
    open_connection.commit()

    open_connection._connection_handler.commit.assert_called_once()

