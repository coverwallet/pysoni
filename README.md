# Pysoni. 
##### Introduction and basic usage. 
Pysoni it is a flexible library that offers short cuts for the most common operations and transactions that a Data Scientist or a Data Engineer could do over a psql database.

Just an example imgine you want to automatically load a DataFrame object from an sql query, mapping all your query datatypes to the correspond DataFrame datatype.With pysoni you could achieve your purpose with just one method.

```python
import pysoni
psy = pysoni.Postgre(port=5432, host='pysoni.host', dbname='pysony.demo', user='pysoni', password='password')

df = psy.postgre_to_dataframe('SELECT * from elephants')
```

Also you could handle other usefull db operations with just a method. Now imagine that you want to update a table following a delete and insert pattern by a table column, controlling equally both transaction delete and insert.

```python
rows_to_insert = [BATCH INSERT]
rows_to_delete = [BATCH TO DELETE]
psy.update_table(tablename=elephants, merge_key=elephant_colour, delete_list=rows_to_delete, insert_list=rows_to_insert, 
insert_batch_size=YOURBATCHINSERTSIZE, delete_batch_size=YOURBATCHDELETESIZE)
```

### Installation.

Pysoni library requires python 3.6 or later versions and is supported for PostgreSQL, versions from 9.2.

Pysoni is available on PyPI. Use pip to install:

```
$ pip install pysoni
```

Additionally you can install directly from github.

```
$ pip install git+ssh://git@github.com/coverwallet/pysoni.git@<tag_name>#egg=pysoni
```
If you are installing pysoni in pipenv enviroment, use the following command.

```
$ pipenv install -e git+ssh://git@github.com/coverwallet/pysoni.git@<tag_name>#egg=pysoni
```

If it is preferred to use http instead of ssh, just change the 'git+ssh' by 'git+http'.

# How to run tests

`docker-compose run --rm tests`
