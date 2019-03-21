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
        self.is_open = False

    def connect(self):
        if self.is_open:
            return True
        
        self._connection_handler = psycopg2.connect(**self._build_connection_arguments())
        self.is_open = True

        return self.is_open

    def close(self):
        if not self.is_open:
            return True
        
        self._connection_handler.close()
        self._connection_handler = None
        self.is_open = False

        return True
        
    def cursor(self):
        return self._connection_handler.cursor()

    def commit(self):
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