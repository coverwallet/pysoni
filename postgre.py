import psycopg2
from psycopg2.extras import execute_values
from time import sleep
from pandas import DataFrame
import asyncpg
from toolz import groupby

class Postgre(object):
    """This class will contain special methods to perform over PostgreSQL.To create a class instance we need
    the following arguments in this order database port, database host, database dbname, database user,
    database password"""

    def __init__(self, port, host, dbname, user, password):
        self.port = port
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password

    @staticmethod
    def format_insert(data_to_insert):
        """This method it is perform to format the output python object into an admissible input for postgresql."""
        data_type = type(data_to_insert[0])
        if data_type is list:
            return [tuple(i) for i in data_to_insert]
        elif data_type is tuple:
            return data_to_insert
        elif data_type in (str,int,float):
            return [tuple([string]) for string in data_to_insert]
        else:
            raise ValueError("Data value not correct")

    @staticmethod
    def read_query(name, path=None):
        """This method it is perform to open an sql query return a python string."""
        if path is None:
            file_location = f"{path}{name}.sql"
        else:
            file_location = f"{name}.sql"

        with open(f'{file_location}.sql', 'r') as query:
            return query.read()

    def connection(self):
        """This method return a postgre connection object."""
        return psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password,
                                host=self.host, port=self.port)

    def delete_batch_rows(self, delete_batch, tablename, column, batch_size=1000, timeout=True):
        """This method it is perform to delete rows in a batch from an specific table, please be careful.
        This method it is think to reduce the amount of database connections"""
        data_type = type(delete_batch[0])
        if data_type not in (str, int):
            raise ValueError('Data format not correct')

        delete_rows, remaining_rows = delete_batch[:batch_size], delete_batch[batch_size:]
        rows_string = ','.join(f"'{register}'" for register in delete_rows)
        while len(delete_rows) > 0:
            self.postgre_statement(f"delete from {tablename} where {column} in ({rows_string})",timesleep=timeout)
            delete_rows, delete_rows = remaining_rows[:batch_size], remaining_rows[batch_size:]
            remaining_rows_amount = str(len(delete_rows))
            print(f"{remaining_rows_amount} rows left to delete")
            if len(delete_rows) > 0:
                rows_string = ','.join(f"'{register}'" for register in delete_rows)
            else:
                break

    def drop_tables(self, tables_names, timesleep=2):
        """This method it is perform to drop tables, please be careful.The tables_names arguments need to be a iterable"""
        conn = self.connection()
        cur = conn.cursor()

        table_type = type(tables_names)
        table_sample = tables_names[0]
        try:
            if table_type in (list, tuple)  and (table_sample in (str,tuple,list)):
                if type(table_sample) in (tuple,list):
                    tables_names = [str(table_name[0]) for table_name in [table for table in tables_names]]
                for table in tables_names:
                    print(f"We delete the following table {table}.Interrupt the script before it's too late.")
                    sleep(timesleep)
                    cur.execute(f'DROP TABLE "{table}";')
                conn.commit()
                print('Tables were successfully removed.')
            else:
                raise ValueError("Data value not correct")
        finally:
            cur.close()
            conn.close()

    def drop_batch_tables(self, tables_names, use_timesleep=True):
        """"This method it is perform to delete a batch of tables, please we aware.
        The tables_names argument need to be an iterable of string"""
        conn = self.connection()
        cur = conn.cursor()

        try:
            table_type = type(tables_names)
            table_sample = tables_names[0]
            if table_type in (list, tuple) and table_sample is str:
                drop_tables = ','.join(table for table in tables_names)
                self.postgre_statement(f"DROP TABLES {drop_tables}", timesleep=use_timesleep)
                conn.commit()
            else:
                raise TypeError("Tablelist parameter not correct, a list or tuple of strings is needed")
        finally:
            cur.close()
            conn.close()

    def execute_batch_inserts(self, insert_rows, tablename, batch_size=1000, columns=None):
        """This method it is created to perform batch insert over postgresql.
        insert_rows has to be an iterable
        table_name is the table where you will insert the data
        batch_size is the amount of rows you'll insert in a DB commit. Be aware that having a huge value here may affect your DB performance
        You can also update only specific columns. To do that, columns has to be either a list, tuple with the column names
        or a string where the columns names are comma-separated"""
        conn = self.connection()
        cur = conn.cursor()
        remaining_rows = self.format_insert(insert_rows)

        if columns:
            if type(columns) in (list, tuple):
                columns_to_insert = ','.join(str(i) for i in columns)
            elif type(columns) is str:
                columns_to_insert = columns
            else:
                raise ValueError('Data format not correct')

            insert_query = f'INSERT INTO {tablename} ({columns_to_insert}) VALUES %s'
        else:
            insert_query = f'INSERT INTO {tablename} VALUES %s'

        batch_rows, remaining_rows = remaining_rows[:batch_size], remaining_rows[batch_size:]
        try:
            while len(batch_rows) > 0:
                execute_values(cur, insert_query, batch_rows)
                batch_rows, remaining_rows = remaining_rows[:batch_size], remaining_rows[batch_size:]
                conn.commit()
        finally:
            cur.close()
            conn.close()


    def execute_query(self, query, types=False, sql_script=None, path_sql_script=None):
        """This method it is perform to execute an sql query.
        If we want to make dynamic queries the attributes should be pass as the following example
        "select * from hoteles where city='{0}'".format('Madrid')"""
        conn = self.connection()
        cur = conn.cursor()
        try:
            if sql_script is None:
                cur.execute(query)
                res = cur.fetchall()
            else:
                cur.execute(self.read_query(sql_script, path_sql_script))
                res = cur.fetchall()
            # we get the information about the columns names.
            columns_names = [column_metadata[0] for column_metadata in cur.description]
            if types is False:
                query_results = {'results': res, 'keys': columns_names}
                return query_results
            else:
                data_types_pid = [column_metadata[1] for column_metadata in cur.description]
                cur.execute(f"select pg_type.oid, pg_type.typname from pg_type where pg_type.oid "
                            f"in ({','.join(str(i) for i in data_types_pid)})")
                
                data_types_information = {type_information[0]: type_information[1] 
                                        for type_information in cur.fetchall()}
                data_types_names = [data_types_information.get(column_type, 'text') for column_type in data_types_information]
                query_results = {'results': res, 'keys': columns_names, 'types': data_types_names}
                return query_results
        finally:
            # we close the cursor and connection.
            cur.close()
            conn.close()

    def get_schema(self, schema, metadata=False):
        """This method it is perform to get all the schema information from postgresql."""
        if metadata is False:
            format_tables = self.postgre_to_tuple(f"select table_name,column_name,data_type "
                                                  f"from information_schema.columns where "
                                                  f"table_schema='{schema}' and "
                                                  f"table_name not like 'pg_%'")
        else:
            format_tables = self.postgre_to_tuple(f"select table_name,column_name,data_type "
                                                  f"from information_schema.columns where "
                                                  f"table_schema='{schema}'")

        if len(format_tables) > 0:
            final_list = [{'tablename': i[0], 'columname': i[1], 'data_type': i[2]} for i in format_tables]
            group_results = groupby('tablename', final_list)
            final_results = []
            for key, value in group_results.items():
                final_results.append({key: {i['columname']: i['data_type'] for i in value}})
            return final_results
        else:
            raise ValueError("This schema it is empty.")

    def postgre_statement(self, statement, timesleep=True):
        """This method it is perform to different postgres statements, rename columns, truncate tables etc."""
        conn = self.connection()
        # we create a cursor
        cur = conn.cursor()
        try:
            if timesleep is False:
                cur.execute(statement)
                print("Statement execute succesfully")
                conn.commit()
                cur.close()
                return print("Statement run succesfully.")
            else:
                cur.execute(statement)
                print("Statement execute succesfully, 10 seconds before")
                sleep(10)
                conn.commit()
                cur.close()
                return print("Statement run succesfully.")
        except psycopg2.Error as e:
            cur.close()
            conn.close()
            raise psycopg2.Error("We found the following issue: {0}"
                                 .format(e))

    def postgre_to_dataframe(self, query):
        """This method it is perform to execute an sql query and it would retrieve a pandas Dataframe.
        If we want to make dynamic queries the attributes should be pass as the following example
        "select * from hoteles where city='{0}'".format('Madrid')"""
        results = self.execute_query(query)
        return DataFrame.from_records(results['results'], columns=results['keys'])

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


class Potgre_Async(object):
    """This class it is going to perform some asynchronous methods over our database, please don´t use it at least
    they are necesarry. For more documentation visit this link. """

    def __init__(self, port, host, dbname, user, password):
        self.port = port
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password

    async def execute_query(self, query):
        conn = await asyncpg.connect(
            user=self.user,
            password=self.password,
            database=self.dbname,
            host=self.host,
            port=self.port,
            ssl=True)
        values = await conn.fetch(query)
        await conn.close()
        return values


class PostgreAdvancedMethods(Postgre):
    """This class it is am abstraction over the Postgre class it is going to give us more specific methods combining
    Postgre methods."""

    def __init__(self, port, host, dbname, user, password):
        super().__init__(port, host, dbname, user, password)
        self.port = port
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password

    def update_table(self, tablename, merge_key, delete_list, insert_list,insert_batch_size=5000,
                     delete_batch_size=False):
        """This method it is perform to update a table, following the delete and insert pattern to avoid unnecesary
        index creation."""
        if delete_batch_size is False:
            self.delete_batch_rows(delete_list, tablename=tablename, column=merge_key, batch_size=insert_batch_size,
                                   timeout=False)
            self.execute_batch_inserts(insert_list, tablename=tablename, batch_size=insert_batch_size)
        else:
            self.delete_batch_rows(delete_list, tablename=tablename, column=merge_key, batch_size=delete_batch_size,
                                   timeout=False)
            self.execute_batch_inserts(insert_list, tablename=tablename, batch_size=insert_batch_size)