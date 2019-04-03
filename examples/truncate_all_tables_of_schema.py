import sys
import os
sys.path.append('..')
from pysoni import Postgre

client = Postgre(port=os.environ['POSTGRES_PORT'], host=os.environ['POSTGRES_HOST'], 
    user=os.environ['POSTGRES_USER'], dbname=os.environ['POSTGRES_DBNAME'],
    password=os.environ['POSTGRES_PASSWORD'], connection_options='-c statement_timeout=30000')

schema_name = "public"
table_names_tuple = client.postgre_to_tuple(
    query=f"SELECT table_name FROM information_schema.tables WHERE table_schema ='{schema_name}';")

table_names = ','.join(f"{schema_name}.{name[0]}" for name in table_names_tuple)

client.postgre_statement(f"TRUNCATE TABLE {table_names}")