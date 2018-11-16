import pytest
from pysoni import Postgre

@pytest.fixture
def pysoni_client():
    return Postgre('','','','','')
    