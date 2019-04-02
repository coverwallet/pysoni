from time import sleep
from random import randrange

from psycopg2.extras import execute_values
from pandas import DataFrame, to_datetime, notnull
from toolz import groupby

from . import helpers
from .connection import Connection


class Postgre(object):
    """This class will contain special methods to perform over PostgreSQL.To create a class instance we need
    the following arguments in this order database port, database host, database dbname, database user,
    database password"""

    def __init__(self, port=None, host=None, dbname=None, user=None, password=None, uri=None,
                 connection_options='-c statement_timeout=3600000'):

        self.db_connection = Connection(port=port, host=host, dbname=dbname,
                                        user=user, password=password, uri=uri,
                                        connection_options=connection_options)

    def connection(self):
        """Generate the DB connection object and connects to it

        The values used during the connection are obtained from the fields
        of the Postgre instance
        """

        self.db_connection.connect()

        return self.db_connection

    def delete_batch_rows(self, delete_batch, table_name, column, batch_size=1000, timeout=True):
        """Delete rows from a table using batches when the table column match any value given in the delete_batch
         argument."""
        helpers.validate_types(subject=delete_batch, expected_types=[list,tuple],
                               contained_types=[str,int])

        delete_batch, remaining_rows = delete_batch[:batch_size], delete_batch[batch_size:]
        while len(delete_batch) > 0:
            rows_string = ','.join(f"'{register}'" for register in delete_batch)
            self.postgre_statement(f"delete from {table_name} where {column} in ({rows_string})", timesleep=timeout)
            delete_batch, remaining_rows = remaining_rows[:batch_size], remaining_rows[batch_size:]


    def drop_tables(self, table_names, wait_time=0, batch=True):
        """Delete a set of DB tables.

        Arguments
        ----------
        tablelist : list, tuple
            Iterable of strings representing the names of the tables to drop
        wait_time : int
            Number of seconds that we'll wait before committing the operation
            to the DB
        batch : boolean
            Defines if the operation should be done in a batch
        """

        helpers.validate_types(table_names, expected_types=[list, tuple], contained_types=[str])

        if batch:
            statement = f"DROP TABLES {', '.join(table_names)}"
            self.postgre_statement(statement, timesleep=wait_time)
        else:
            for table_name in table_names:
                self.postgre_statement(f"DROP TABLE {table_name}", timesleep=wait_time)


    def execute_batch_inserts(self, insert_rows, tablename, batch_size=1000):
        """Insert rows in a table using batches, the default batch size it is set to 1000."""
        helpers.validate_types(subject=insert_rows, expected_types=[list, tuple], contained_types=[str, int, float, list, tuple])

        conn = self.connection()
        cur = conn.cursor()
        try:
            insert = helpers.format_insert(insert_rows)
            batch_insert, insert = insert[:batch_size], insert[batch_size:]
            while batch_insert:
                execute_values(cur, f'INSERT INTO {tablename}' + ' VALUES %s', 
                               batch_insert)
                batch_insert, insert = insert[:batch_size], insert[batch_size:]
                conn.commit()
        finally:
            conn.commit()
            cur.close()
            conn.close()

    def execute_batch_inserts_specific_columns(self, insert_rows, tablename, columns, batch_size=1000):
        """Insert rows in spectific table columns using batches, the default batch size it is set to 1000."""
        helpers.validate_types(subject=columns, expected_types=[list, tuple, str])
        helpers.validate_types(subject=insert_rows, expected_types=[list, tuple], contained_types=[str, int, float, list, tuple])

        conn = self.connection()
        cur = conn.cursor()
        try:      
            insert_colums = helpers.format_sql_string(subject=columns)            
            insert = helpers.format_insert(insert_rows)
            batch_insert, insert = insert[:batch_size], insert[batch_size:]
            while batch_insert:
                execute_values(cur, f'INSERT INTO {tablename} ({insert_colums}) '
                                    +' VALUES %s', batch_insert)
                batch_insert, insert = insert[:batch_size], insert[batch_size:]
                conn.commit()
        finally:
            conn.commit()
            cur.close()
            conn.close()

    def execute_query(self, queryname, types=False, path_sql_script=None):
        """This method is used to execute an sql query. With default parameters, it returns a dictionary
        with two keys: 'results' provides a list with the results of the query and 'keys' provides a list
        with the names of each of the columns returned. If types = true, it also returns a key 'types' inside
        the dictionary that contains the type of each of the columns. If a path in "path_sql_script" is 
        specified, the query is read from the file named 'queryname' (with the SQL query) that is located 
        in that specific path is used.
        If we want to make dynamic queries, the attributes should be passed as in the following example:
        place = "Madrid"
        f"select * from hotels where city='{place}'" """
        conn = self.connection()
        cur = conn.cursor()
        column_name_index = 0
        column_type_index = 1
        try:
            query_results = {}
            if path_sql_script:
                cur.execute(helpers.read_query(queryname, path_sql_script))
            else:
                cur.execute(queryname)
            cursor_info = cur.fetchall()
            columns_names = [cursor_metadata[column_name_index] for cursor_metadata in cur.description]
            if types:
                types_of_columns = [cursor_metadata[column_type_index] for cursor_metadata in cur.description]
                type_string = ','.join(str(type_code) for type_code in types_of_columns)
                cur.execute(f"select pg_type.oid, pg_type.typname from pg_type where pg_type.oid in ({type_string})")
                list_of_types = cur.fetchall()
                oid_name_type_dict = {type_column_tuple[0]: type_column_tuple[1] for type_column_tuple in list_of_types}
                type_name_list = [oid_name_type_dict.get(type_code, 'text') for type_code in types_of_columns]
                query_results = {'results': cursor_info, 'keys': columns_names, 'types': type_name_list}
            else:
                query_results = {'results': cursor_info, 'keys': columns_names}
            return query_results  
        
        finally:
            cur.close()
            conn.close()
            

    def get_schema(self, schema, metadata=False):
        """This method it is perform to get all the schema information from postgresql."""

        query = (
            "SELECT table_name, column_name, data_type FROM "
            f"information_schema.columns WHERE table_schema='{schema}'"
        )
        if not metadata:
            query += "AND table_name NOT LIKE 'pg_%'"

        tables_information = self.postgre_to_tuple(query)

        if not tables_information:
            raise ValueError('The schema is empty')

        schemas_metadata = [{
            'table_name': table_information[0],
            'column_name': table_information[1],
            'data_type': table_information[2]
        } for table_information in tables_information]

        final_results = []
        for table_name, table_metadatas in groupby('table_name', schemas_metadata).items():
            final_results.append({
                table_name: { metadata['column_name']: metadata['data_type'] for metadata in table_metadatas }
            })

        return final_results


    def postgre_statement(self, statement, timesleep=0):
        """Method to perform postgres transactions as an example rename columns,
        truncate tables, grant permissions to users etc,.
        Arguments
        ----------
        statement : string
            String representing the transacion we want to execute
        timesleep : int
            Number with the seconds we want to wait between the transaction is  
            executed and commited time.
        """
        conn = self.connection()
        cur = conn.cursor()
        try:
            cur.execute(statement)
            sleep(timesleep)
        finally:
            conn.commit()
            cur.close()
            conn.close()

    def postgre_multiple_statements(self, statements, timesleep=0):
        """Method to perform multiple postgres transactions as an example rename columns,
        truncate tables, grant permissions to users etc..All transactions are commited
        at the same time.
        Arguments
        ----------
        statements : list, tuple
            Iterable of strings representing the transacion.
            All transactions are executed following the order of the iterable
        timesleep : int
            Number with the seconds we want to wait between alls transactions are 
            executed and commited time.
        """

        helpers.validate_types(statements, expected_types=[list, tuple], contained_types=[str])

        self.postgre_statement(";".join(statements), timesleep=timesleep)

    def dataframe_to_postgre(self, tablename, dataframe_object, method, batch_size, 
                               merge_key=None):
        """This method it is perform to insert a Dataframe python object into a DWH table.
        The insert method can be done by appending elements to a table for that purpose use
        the append opction in the method param. If you want to update a table by a column, you 
        need to use the rebuilt method and select the merge_key column of your DWH table.
        """
        dataframe_object = dataframe_object.where((notnull(dataframe_object)), None)
        df_columns = dataframe_object.columns[0:].tolist()
        df_values = dataframe_object.values[:, 0:].tolist()

        if method not in ('rebuild', 'append'):
            raise ValueError("""Invalid method. Select method='rebuild' if you
                want to update a table using a column. Select method='append'
                if you want just to update it.""")

        if method == 'rebuild':
            if not merge_key:
                raise ValueError("""To rebuilt a table you must select a
                    merge_key with the table column""")

            df_delete_values = dataframe_object[merge_key].unique().tolist()
            self.update_table(tablename=tablename, merge_key=merge_key,
                insert_batch_size=batch_size, delete_batch_size=batch_size,
                insert_list=df_values, delete_list=df_delete_values,
                columns=df_columns)

        elif method == 'append':
            self.execute_batch_inserts_specific_columns(
                tablename=tablename, columns=df_columns, insert_rows=df_values,
                batch_size=batch_size)


    def postgre_to_dataframe(self, query, convert_types=True, path_sql_script=None):
        """This method is used to execute a sql query and return a pandas Dataframe with the results.
        If 'convert_types' = True, the time variables are converted to timestamp format and the date variables
        are converted to YYYY-MM-DD format.
        If we want to make dynamic queries the attributes should be pass as the following example
        place = "Madrid"
        f"select * from hotels where city='{place}'" """
        results = self.execute_query(query, types=convert_types,
                                     path_sql_script=path_sql_script)
        df = DataFrame.from_records(results['results'], columns=results['keys'])

        if convert_types:
            for column_data_type, column_name in zip(results['types'], results['keys']):
                if column_data_type in ('timestamp', 'timestamptz'):
                    df[column_name] = to_datetime(df[column_name])
                elif column_data_type == 'date':
                    df[column_name] = to_datetime(
                        df[column_name], format='%Y-%m-%d')
        return df

    def postgre_to_dict(self, query, types=False, path_sql_script=None):
        """This method is used to execute an sql query and it would retrieve a
        list, corresponding each element to a different row of the resulted 
        query. Each element is in turn made up of a list of dictionaries in
        which the keys are the name of the columns and the value is the the 
        actual value of the row for that specific column. If types=True, it 
        also returns the type of each column inside each dictionary."""
        
        results = self.execute_query(query, types=types,
                        path_sql_script=path_sql_script)
        columns = results['keys']
        rows = results['results']

        if types:
            types = results['types']
            list_of_dict = []
            for row in rows:
                list_of_dict.append([{column: {'value': value, 'type': type_}} 
                    for value, column, type_ in zip(row, columns, types)])
            return list_of_dict
        else:
            rows = results['results']
            list_of_dict = []
            for row in rows:
                list_of_dict.append([{column: register} for register, 
                    column in zip(row, columns)])
            return list_of_dict

    def postgre_to_dict_list(self, query, types=False, path_sql_script=False):
        """This method is used to execute an sql query and return a list of 
        dictionaries. Each dictionary contais the information of each element
        of the result (value + column). If types=True, the dictionary  also
        includes the type of the column.
        Different from 'postgre_to_dict', here each element of each row has
        its own dictionary, and inside the dictionary it is contained the 
        value, the name of the column and the type of the column (if True)"""

        results = self.execute_query(query, types=types, path_sql_script=path_sql_script)
        columns, rows = results['keys'], results['results']

        if types:
            types = results['types']

            list_of_dict = []
            for row in rows:
                for value, column, type_ in zip(row, columns, types):
                    list_of_dict.append({column: {'value': value, 'type': type_}})
            return list_of_dict
        else:
            list_of_dict = []
            for row in rows:
                row_dict = {}
                for value, column in zip(row, columns):
                    row_dict.update({column: value})
                list_of_dict.append(row_dict)
            return list_of_dict

    def postgre_to_tuple(self, query, path_sql_script=False):
        """This method it is perform to execute an sql query and it would retrieve a list of tuples.
        If we want to make dynamic queries the attributes should be pass as the following example
        f"select * from hoteles where city='Madrid'")"""

        results = self.execute_query(query, path_sql_script=path_sql_script)
        return results['results']

    def update_fields(self, tablename, column, values, wait_time=0):
        """Method to perform updates over a column.
        Arguments
        ----------
        tablename : string
            String representing the table we want to update.
        column : int
            String representing the column we want to update the values.
        values : list, tuple
            Iterable of iterables representing the values we want to update.
            By default the first element of the value is corresponding with,
            the old value and the second element of the value corresponds with,
            the new value we are going to update.
        wait_time : int
            Sleep time between update transactions.
        """

        helpers.validate_types(values, expected_types=[list, tuple], 
                               contained_types=[list,tuple])

        OLD_RECORD_INDEX = 0
        NEW_RECORD_INDEX = 1

        for record in values: 
            update = (f"UPDATE {tablename} "
                     f"SET {column}='{record[NEW_RECORD_INDEX]}' "
                     f"WHERE {column}='{record[OLD_RECORD_INDEX]}'")
            self.postgre_statement(update, timesleep=wait_time)

    def update_fields_in_batch(self, tablename, column, values, batch_size):
        """Method to perform postgres batch updates over a column.
        Arguments
        ----------
        tablename : string
            String representing the table we want to update.
        column : int
            String representing the column we want to update the values.
        values : list, tuple
            Iterable of iterables representing the values we want to update.
            By default the first element of the value is corresponding with,
            the old value and the second element of the value corresponds with,
            the new value we are going to update.
        wait_time : int
            Sleep time between update transactions.
        """
        
        helpers.validate_types(values, expected_types=[list, tuple],
                               contained_types=[list, tuple])

        conn = self.connection()
        cur = conn.cursor()
        try:
            batch_update, update = values[:batch_size], values[batch_size:]
            while len(batch_update) > 0:
                    execute_values(cur, f"UPDATE {tablename} SET {column}" 
                                        +"= data.new_value FROM (VALUES %s) "
                                        "AS data (old_value, new_value) " \
                                        f"WHERE {column} = data.old_value",
                                   batch_update)
                    batch_update, update = update[:batch_size], update[batch_size:]
        finally:
            conn.commit()
            cur.close()
            conn.close()
            
    def update_table(self, tablename, merge_key, delete_list, insert_list, insert_batch_size=5000,
                     delete_batch_size=None, columns=None):
        """Update the records of a DB table in batches

        It follows the delete-and-insert pattern (first delete all the rows
        that will be updated, then insert them with the new values) because
        this greatly improves the speed over doing the update row by row, as
        this pattern enables batch operations on the DB.

        WARNING: It's important to consider that if only specific columns are
        updated (by using the 'columns' argument) the rest of the values of the
        row will be lost (as they won't be re-inserted after the deletion)

        Arguments
        ---------
        tablename : string
            Name of the table that will be updated
        merge_key : string
            Name of the column that will be updated
        delete_list : list, tuple
            Iterable containing the table PK of the rows that we want to remove
        insert_list : list, tuple
            Iterable of iterables, representing all the values that will be
            inserted in each row
        insert_batch_size : integer
            Size of the batch to insert in each DB transaction
        delete_batch_size : integer
            Size of the batch to delete in each DB transaction. If not
            specified, it's set to the same value as insert_batch_size
        columns : list, tuple
            Columns to update, in case we don't want to set all the values
            in the record.
            WARNING: If you use this option, the values on the missing columns
            will be lost
        """

        if not delete_batch_size:
            delete_batch_size = insert_batch_size

        self.delete_batch_rows(
            delete_list, table_name=tablename, column=merge_key,
            batch_size=delete_batch_size, timeout=False
        )

        if columns:
            self.execute_batch_inserts_specific_columns(
                insert_list, tablename=tablename,
                batch_size=insert_batch_size, columns=columns
            )

        else:
            self.execute_batch_inserts(
                insert_list, tablename=tablename, batch_size=insert_batch_size
            )
