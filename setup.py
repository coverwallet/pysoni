from setuptools import setup

meta = {}
exec(open('./src/version.py').read(), meta)

setup(name='pysoni',
      description='Python library for psql clients',
      version=meta['__version__'],
      author='Coverwallet Data Team',
      license='MIT',
      packages=['src'],
      install_requires=[
          'psycopg2',
          'pandas',
          'toolz',
          'psycopg2-binary'
      ])
