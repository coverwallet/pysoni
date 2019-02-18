from time import sleep
from random import randrange

import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import parse_dsn
from pandas import DataFrame, to_datetime
from toolz import groupby

from . import helpers


class Postgre(object):
    """This class will contain special methods to perform over PostgreSQL.To create a class instance we need
    the following arguments in this order database port, database host, database dbname, database user,
    database password"""

    def __init__(self, port=None, host=None, dbname=None, user=None, password=None, uri=None,
                 connection_options='-c statement_timeout=3600000'):
        if not uri:
            self.port = port
            self.host = host
            self.dbname = dbname
            self.user = user
            self.password = password
            self.connection_options = connection_options
        else:
            uri_dict = parse_dsn(uri)
            
            self.port = uri_dict.get('port')
            self.host = uri_dict.get('host')
            self.dbname = uri_dict.get('dbname')
            self.user = uri_dict.get('user')
            self.password = uri_dict.get('password')
            self.connection_options = connection_options

    @staticmethod
    def format_insert(data_to_insert):
        """Translates the python object output into an admisible postgresql input."""
        data_type = type(data_to_insert[0])

        if data_type is list:
            return [tuple(value) for value in data_to_insert]
        elif data_type is tuple:
            return data_to_insert
        elif data_type in (str, int, float):
            return [tuple([value]) for value in data_to_insert]
        else:
            raise ValueError("Data type is not correct")

    @staticmethod
    def read_query(name, path=None):
        """This method it is perform to open an sql query return a python string."""
        if path:
            filename = f"{path}{name}.sql"
        else:
            filename = f"{name}.sql"

        with open(filename, 'r') as query:
            return query.read()

    def connection(self):
        """This method return a postgre connection object."""
        if self.connection_options:
            return psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password,
                                host=self.host, port=self.port, options=self.connection_options)
        else:
            return psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password,
                                    host=self.host, port=self.port)

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


    def drop_tables(self, table_names, timesleep=2):
        """Drop tables from a database sequentially, timesleep between transactions it is set up to 2 seconds by default,
        all transactions are commited at the same time"""
        conn = self.connection()
        cur = conn.cursor()

        table_type = type(table_names)
        sample_type = table_names[0]

        if not table_type in (list, tuple) or not (sample_type is str):
            raise ValueError("Input data doesn't have the correct format. It should be a list/tuple of strings")

        try:
            for table in table_names:
                print(f"We delete the following table {table}.Interrupt the script before it's too late.")
                sleep(timesleep)
                cur.execute(f'DROP TABLE "{table}";')

            conn.commit()
            print('Tables were successfully removed')

        finally:
            cur.close()
            conn.close()

    def drop_batch_tables(self, tablelist, timeout=True):
        "This method it is perform to delete a batch of tables , please we aware."
        if type(tablelist) in (list, tuple) and tablelist[0] is str:
            drop_tables = ','.join(table for table in tablelist)
            self.postgre_statement(f"DROP TABLES {drop_tables}", timesleep=timeout)
        else:
            raise TypeError("Tablelist parameter not correct, a list or tuple of strings is needed")

    def execute_batch_inserts(self, insert_rows, tablename, batch_size=1000):
        """Insert rows in a table using batches, the default batch size it is set to 1000."""
        conn = self.connection()
        cur = conn.cursor()
        try:
            insert = self.format_insert(insert_rows)
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
        conn = self.connection()
        cur = conn.cursor()
        try:      
            helpers.validate_types(subject=columns, expected_types=[list, tuple, str])

            insert_colums = helpers.format_sql_string(subject=columns)            
            insert = self.format_insert(insert_rows)
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

    def execute_query(self, queryname, types=False, sql_script=None, path_sql_script=None):
        """This method is used to perform to execute an sql query.
        If we want to make dynamic queries the attributes should be pass as the following example
        select * from hoteles where city='{0}'".format('Madrid')"""
        conn = self.connection()
        cur = conn.cursor()
        query_results = {}
        column_name_index = 0
        column_type_index = 1
        try:
            if not sql_script:
                cur.execute(queryname)
            else:
                cur.execute(self.read_query(queryname, path_sql_script))
            cursor_info = cur.fetchall()
            columns_names = [cursor_metadata[column_name_index] for cursor_metadata in cur.description]
            if types is False:
                query_results = {'results': cursor_info, 'keys': columns_names}
            else:
                types_of_columns = [cursor_metadata[column_type_index] for cursor_metadata in cur.description]
                type_string = ','.join(str(type_code) for type_code in types_of_columns)
                cur.execute(f"select pg_type.oid, pg_type.typname from pg_type where pg_type.oid in ({type_string})")
                list_of_types = cur.fetchall()
                oid_name_type_dict = {type_column_tuple[0]: type_column_tuple[1] for type_column_tuple in list_of_types}
                type_name_list = [oid_name_type_dict.get(type_code, 'text') for type_code in types_of_columns]
                query_results = {'results': cursor_info, 'keys': columns_names, 'types': type_name_list}
                
        finally:
            cur.close()
            conn.close()
            return query_results

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
        """Method to perform  postgres transactions as an example rename columns, truncate tables etc. By default
        the transaction it is commited after the execution if you want set up a sleep between both events or
        different transactions use the timesleep parameter"""
        conn = self.connection()
        cur = conn.cursor()
        try:
            cur.execute(statement)
            print("Statement execute succesfully")
            sleep(timesleep)
            conn.commit()
            print("Statement run succesfully")

        finally:
            cur.close()
            conn.close()

    def postgre_multiple_statements(self, statements, timesleep=None):
        """Method to execute multiple db transactions. The transactions are executed sequentially.
        A list of string with the transactions we want to execute need to be we passed on the statements argument.
        You can use a sleep between transactions with the timesleep parameter.
        All peding transaction will be commited before any expection it is raised."""

        statement_type = type(statements)
        sample = statements[randrange(0, len(statements))]
        if statement_type is list and type(sample) is str:
            raise TypeError('Statements argument need to be a list of strings')

        if not timesleep:
            timesleep = 0
        statement_counter = 0

        conn = self.connection()
        cur = conn.cursor()
        try:
            for statement in statements:
                cur.execute(statement)
                sleep(timesleep)
                statement_counter += 1
        except Exception as e:
            unresolved_statements = '; '.join(statement for statement in statements[statement_counter:])
            print(f"Script failed in this transaction {statements[statement_counter]}.")
            print("The following transactions were not executed: "
                  f"{unresolved_statements}")
            print(repr(e))
        finally:
            conn.commit()
            cur.close()
            conn.close()

    def dataframe_to_postgre(self, tablename, dataframe_object, method, batch_size, 
                               merge_key=None):
        """This method it is perform to insert a Dataframe python object into a DWH table.
        The insert method can be done by appending elements to a table for that purpose use
        the append opction in the method param. If you want to update a table by a column, you 
        need to use the rebuilt method and select the merge_key column of your DWH table."""

        df_columns = dataframe_object.columns[0:].tolist()
        df_values = dataframe_object.values[:, 0:].tolist()

        if method not in ('rebuilt', 'append'):
            raise ValueError("""Invalid method. Choose rebuild method if you want to 
                                 update a table using a column, choose append if you want 
                                just to update it.""")

        if method == 'rebuilt':
            if not merge_key:
                raise ValueError("""To rebuilt a table you must select 
                                 merge_key with the table column""")

            df_delete_values = dataframe_object[merge_key].unique().tolist()
            self.update_table(tablename=tablename, merge_key=merge_key,
                              insert_batch_size=batch_size, delete_batch_size=batch_size,
                              insert_list=df_values, delete_list=df_delete_values, 
                              columns=df_columns)

        elif method == 'append':
            self.execute_batch_inserts_specific_columns(tablename=tablename, columns=df_columns,
                                                        insert_rows=df_values, batch_size=batch_size)


    def postgre_to_dataframe(self, query, convert_types=True, sql_script=None, path_sql_script=None):
        """This method it is perform to execute an sql query and it would retrieve a pandas Dataframe.
        If we want to make dynamic queries the attributes should be pass as the following example
        "select * from hoteles where city='{0}'".format('Madrid')"""
        results = self.execute_query(query, types=convert_types, sql_script=sql_script,
                                     path_sql_script=path_sql_script)
        df = DataFrame.from_records(results['results'], columns=results['keys'])

        if convert_types:
            for column_data_type, column_name in zip(results['types'], results['keys']):
                if column_data_type in ('timestamp', 'timestampz'):
                    df[column_name] = to_datetime(df[column_name])
                elif column_data_type == 'date':
                    df[column_name] = to_datetime(
                        df[column_name], format='%Y-%m-%d')
        return df

    def postgre_to_dict(self, query, types=False, sql_script=None, path_sql_script=None):
        """This method it is perform to execute an sql query and it would retrieve a list of lists of diccionaries.
        If we want to make dynamic queries the attributes should be pass as the following example
        "select * from hoteles where city='{0}'".format('Madrid')"""
        if types is False:
            results = self.execute_query(query, sql_script=sql_script, path_sql_script=path_sql_script)
            columns = results['keys']
            rows = results['results']
            list_of_dict = []

            for row in rows:
                list_of_dict.append([{column: register} for register, column in zip(row, columns)])
            return list_of_dict
        else:
            results = self.execute_query(query, types=True, sql_script=sql_script, path_sql_script=path_sql_script)
            columns = results['keys']
            rows = results['results']
            types = results['types']
            list_of_dict = []
            for row in rows:
                list_of_dict.append([{column: {'value': value, 'type': type_}} for value, column, type_
                                     in zip(row, columns, types)])
            return list_of_dict

    def postgre_to_dict_list(self, query, types=False, sql_script=False, path_sql_script=False):
        """This method it is perform to execute an sql query and it would retrieve a list of lists of diccionaries.
        If we want to make dynamic queries the attributes should be pass as the following example
        f"select * from hoteles where city='Madrid'"""
        if types is False:
            results = self.execute_query(query, sql_script=sql_script, path_sql_script=path_sql_script)
            columns = results['keys']
            rows = results['results']
            list_of_dict = []
            for row in rows:
                row_dict = {}
                for register, column in zip(row, columns):
                    row_dict.update({column: register})
                list_of_dict.append(row_dict)
            return list_of_dict
        else:
            results = self.execute_query(query, types=True, sql_script=sql_script, path_sql_script=path_sql_script)
            columns = results['keys']
            rows = results['results']
            types = results['types']
            list_of_dict = []
            for row in rows:
                for value, column, type_ in zip(row, columns, types):
                    list_of_dict.append({column: {'value': value, 'type': type}})
            return list_of_dict

    def postgre_to_tuple(self, query, sql_script=False, path_sql_script=False):
        """This method it is perform to execute an sql query and it would retrieve a list of tuples.
        If we want to make dynamic queries the attributes should be pass as the following example
        f"select * from hoteles where city='Madrid'")"""

        results = self.execute_query(query, sql_script=sql_script, path_sql_script=path_sql_script)
        return results['results']

    def update_fields(self, tablename, field, values, timeout=True, multiproccesing=False):
        """This method it is perform to create massive updates over a table, you could use the multiprocessing flag to
        open parallel connections."""
        if multiproccesing is False:
            if type(values) != (list, tuple) and len(values[0]) != 2:
                raise TypeError("Values argument need to be a list or tuple of lists or tuples first element"
                                "old value second element new value")
            else:
                pass
            for record in values:
                self.postgre_statement(f"UPDATE {tablename} SET {field} = '{record[1]}' WHERE {field}='{record[0]}'",
                                       timesleep=timeout)
            return "fields updated correctly"
        else:
            if type(values) != (list, tuple) and len(values) != 2:
                raise TypeError("Values argument need to be a list or tuple of lists or tuples first element"
                                "old value second element new value")
            else:
                self.postgre_statement(f"UPDATE {tablename} SET {field} = '{values[1]}' WHERE {field}='{values[0]}'",
                                       timesleep=timeout)
            return "fields updated correctly"

    def update_fields_execute_values(self, tablename, field, values_tuple, batch_size):
        """This method it is perform to create massive updates using execute_values method"""
        if len(values_tuple[0]) == 2 and type(values_tuple[0]) in (tuple, list) and type(values_tuple[0][1]) is str:
            pass
        else:
            raise TypeError("Value error value tuple argument need to be a list of tuples of longitude 2 where "
                            "each element of the tuple need to be a string.")
        conn = self.connection()
        # we create a cursor
        cur = conn.cursor()
        try:
            batch_update, update = values_tuple[:batch_size], values_tuple[batch_size:]
            while len(batch_update) > 0:
                try:
                    execute_values(cur, "UPDATE " + tablename + " SET " + field + " = data.new_value FROM (VALUES %s) "
                                                                                  "AS data (old_value, new_value) " \
                                                                                  "WHERE " + field + " = data.old_value",
                                   batch_update
                                   )
                    batch_update, update = update[:batch_size], update[batch_size:]
                    conn.commit()
                except Exception as e:
                    print(e)
                    break
            print("fields updated correctly")
        except:
            cur.close()
        finally:
            conn.close()
            
    def update_table(self, tablename, merge_key, delete_list, insert_list, insert_batch_size=5000,
                     delete_batch_size=None, columns=None):
        """This method it is perform to update a table, following the delete and insert pattern to avoid unnecesary
        index creation. If you don't specify nothing in delete_batch_size arguments the method it is going to delete
        values of the same size of the insertion"""
        if delete_batch_size:
            self.delete_batch_rows(delete_list, table_name=tablename, column=merge_key, batch_size=delete_batch_size,
                                   timeout=False)
            if columns:
                self.execute_batch_inserts_specific_columns(insert_list, tablename=tablename, batch_size=insert_batch_size,
                                            columns=columns)
            else:
                self.execute_batch_inserts(insert_list, tablename=tablename, batch_size=insert_batch_size)
        else:
            self.delete_batch_rows(delete_list, table_name=tablename, column=merge_key, batch_size=insert_batch_size,
                                   timeout=False)
            if columns:
                self.execute_batch_inserts_specific_columns(insert_list, tablename=tablename, batch_size=insert_batch_size,
                                                            columns=columns)
            else:
                self.execute_batch_inserts(insert_list, tablename=tablename, batch_size=insert_batch_size)
