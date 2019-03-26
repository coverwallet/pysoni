import psycopg2
from psycopg2.extensions import parse_dsn


class Connection:
    def __init__(self, port=None, host=None, dbname=None, user=None, password=None, uri=None,
                 connection_options=None):
        if uri:
            uri_dict = parse_dsn(uri)
            
            self.port = uri_dict.get('port')
            self.host = uri_dict.get('host')
            self.dbname = uri_dict.get('dbname')
            self.user = uri_dict.get('user')
            self.password = uri_dict.get('password')
        
        else:
            self.port = port
            self.host = host
            self.dbname = dbname
            self.user = user
            self.password = password

        self.connection_options = connection_options
        self._connection_handler = None

    def connect(self):
        """Start the connection to the DB

        Calling to this method will start the connection to the DB, using the
        arguments specified on the object's initialization.

        The underlying connection is managed via the psycopg2 library. If the
        connection is stablished correctly, the psycopg2 connection will be
        assigned to the field `_connection_handler` of the Connection instance.
        Please note that this field is intended to be for internal use only, 
        and that you shouldn't access it directly from your app.
        """
        self._connection_handler = psycopg2.connect(**self._build_connection_arguments())

    def close(self):
        """Close the DB connection

        Delegates the `close()` method to the psycopg2 connection handler and
        then sets the `_connection_handler` field to None
        """
        self._connection_handler.close()
        self._connection_handler = None
        
    def cursor(self):
        """Obtain a psycopg2 DB cursor"""
        return self._connection_handler.cursor()

    def commit(self):
        """Commit all the changes pending on this connection to the DB"""
        return self._connection_handler.commit()

    def _build_connection_arguments(self):
        connection_arguments = {
            'dbname': self.dbname,
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'port': self.port,
            'options': self.connection_options
        }

        return connection_arguments