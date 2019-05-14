import psycopg2
from psycopg2.extensions import parse_dsn


class Connection:
    """Class to represent database connecions

    Instances of this class will contain the logic to do the operations related
    specifically with the DB connection: opening/closing, obtaining cursors,
    commiting changes...
    """

    def __init__(self, port=None, host=None, dbname=None, user=None, password=None, uri=None,
                 connection_options=None):
        """
        Parameters
        -----------
            port : int
                Port of the host in which the DB is exposed
            host : str
                Address of the host in which the DB is running
            dbname : str
                Name of our DB
            user : str
                Username to use to identify in the DB
            password : str
                Password to use to identify in the DB
            uri : str
                URI of the DB we want to connect to. If it's included, the DB
                connection arguments will be parsed from it and the rest of the
                arguments will be ignored.
            connecion_options : str
                Additional options we may want to specify when connecting to
                the DB. Its value will be sent directly to psycopg2.connect
        """

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
        self._is_opened = False
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

        if self._is_opened:
            return

        self._connection_handler = psycopg2.connect(**self._build_connection_arguments())
        self._is_opened = True

    def close(self):
        """Close the DB connection

        Delegates the `close()` method to the psycopg2 connection handler and
        then sets the `_connection_handler` field to None
        """

        if not self._is_opened:
            return

        self._connection_handler.close()
        self._connection_handler = None
        self._is_opened = False
        
    def cursor(self):
        """Obtain a psycopg2 DB cursor

        Delegates the call to the psycopg2 connection"""

        return self._connection_handler.cursor()

    def commit(self):
        """Commit all the changes pending on this connection to the DB

        Delegates the call to the psycopg2 connection"""

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