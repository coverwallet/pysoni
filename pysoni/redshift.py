from typing import List

from pysoni import Postgre


class Redshift(Postgre):
    """This class will contain some special methods to execute over an AWS Redshift DWH.

    AWS Redshift use the same dialect than PostgreSQL, so all the functionality within the Postgre
    class can be apply directly to AWS Redshift. Althougth mainly all the functionality of this class
    can not we used over a PostgreSQL DB.
    """

    DEFAULT_SCHEMA = 'public'

    def __init__(self, aws_access_key_id: str, aws_access_secret_key: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_access_key_id = aws_access_key_id
        self.aws_access_secret_key = aws_access_secret_key

    @property
    def aws_creds(self):
        """Property to format aws credentials into a valid string to be used in redshift transactions"""
        return f"aws_access_key_id={self.aws_access_key_id};aws_secret_access_key={self.aws_access_secret_key}"

    def generate_s3_to_redshift_copy_statement(
            self, s3_path: str, table_name: str, columns: List[str] = None, copy_options: List[str] = None,
            table_schema: str = DEFAULT_SCHEMA) -> str:
        """Method to generate a valid copy statement to load data directly from s3 to Redshit.
           Docs here: https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html


           Arguments
           ---------
           s3_path: str
               Path where we can find the file we want to load in s3.
           table_name: str
               Name of the db table where we want to load the data.
           columns: list
               List of strings in order with the columns of the db object where we
               want to load the data. The order of list with the columns must follow the order
               of the file columns, in case we want to load a csv file.
               (default is None)
           copy_options: list
               List of strings with the additional functionality we want to apply when we copy
               the file.(see docs https://docs.aws.amazon.com/redshift/latest/dg/r_COPY-parameters.html)
               (default is None)
           table_schema: str
               Name of the db schema where we are going to load the data.
               (default is public)


           Returns
           -------
           string
               a string with a copy statement executable over a Amazon Redshift DWH.
        """

        format_copy_columns = f"({','.join(columns)})" if columns else ''
        format_db_path = f"{table_schema}.{table_name}" if table_schema else table_name
        if not copy_options:
            copy_options = []

        return (f"COPY {format_db_path} {format_copy_columns} "
                f"FROM '{s3_path}' "
                f"with credentials '{self.aws_creds}' "
                f"{' '.join(copy_options)}")

    def update_table_from_s3(
            self, s3_path: str, table_name: str, tmp_table_name: str, columns: List[str] = None,
            copy_options: List[str] = None, table_schema: str = DEFAULT_SCHEMA, delete_statement: str = None,
            posthook_statement: str = None):
        """Method update a redshift db table loading data from s3. They idea of this method it is
           to provide a simple interface where multiple different update approaches can be accomplish,
           (TRUNCATE and INSERT, DELETE and INSERT, INSERT).so that's why in this case the delete statement
           it is a really open argument where the developer it has the responsability
           of performing a valid and correct delete transaction before inserting new data.

           This method it is going to update the db object using a batch approach, trying to use the
           less db statements as possible.
           The connection must be persistent (is_persistent=True) to use temporary
           tables.

           WARNING: It's important to consider that if only specific columns are
           updated (by using the 'columns' argument) the rest of the values of the
           row will be lost (as they won't be re-inserted after the deletion)

           Arguments
           ---------
           s3_path: str
               Path where we can find the file we want to load in s3.
           table_name: str
               Name of the db table where we want to load the data.
           tmp_table_name: str
               Name of a temp table use it to load data from s3 before updating the
               target table.This table it is going to be delete automatically after
               the db connection it is closed although,it is advisable to give a unique
               name to this table.
           columns: list
               List of strings in order with the columns of the db object where we
               want to load the data. The order of list with the columns must follow the order
               of the file columns, in case we want to load a csv file (default is None).
           copy_options: list
               List of strings with the additional functionality we want to apply when we copy
               the file.(see docs https://docs.aws.amazon.com/redshift/latest/dg/r_COPY-parameters.html)
               (default is None).
           table_schema: str
               Name of the db schema where we are going to load the data (default is None)
           delete_statement: str
               String, with the delete transaction we want to execute over the db object, before
               updating it with new data. Multiple transactions can be executed.
               Ex: TRUNCATE TABLE public.accounts
               EX: DELETE FROM public.accounts using accounts_temp
                   WHERE accounts.id = accounts_temp.id
               (default is None)
           posthook_statement: str
               String, with a db valid transaction which it is going to be execute once the
               target table it is updated. Use this parameter to execute management transactions
               over the target db object as an example give permission to this table once it is updated
               to a user or a group of users.

        """

        if not self.db_connection.is_persistent:
            raise ConnectionError("The connection must be persistent to use temporary tables")

        format_db_select_columns = f"{','.join(columns)}" if columns else '*'
        format_db_insert_columns = f"({','.join(columns)})" if columns else ''
        format_db_path = f"{table_schema}.{table_name}"

        db_create_tmp_table_statement = (
            f"CREATE TEMP TABLE {tmp_table_name} AS "
            f"(SELECT {format_db_select_columns} FROM {format_db_path} LIMIT 0)")

        copy_statement = self.generate_s3_to_redshift_copy_statement(
            s3_path=s3_path, table_name=tmp_table_name,
            columns=columns, copy_options=copy_options, table_schema=None)

        db_insert_statement = (
            f"INSERT INTO {format_db_path}{format_db_insert_columns} "
            f"(SELECT {format_db_select_columns} FROM {tmp_table_name})")

        statements_to_execute = [
            db_create_tmp_table_statement, copy_statement, delete_statement,
            db_insert_statement, posthook_statement]

        statement = ';'.join(filter(None, statements_to_execute))

        self.postgre_statement(statement)
