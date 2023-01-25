import pytest
import os
import random
import teradatasql
from datetime import datetime
#from dotenv import load_dotenv, find_dotenv
#from pathlib import Path

#load_dotenv("../test.env")

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be locals
pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    hostname='localhost'
    username='dbc'
    password='dbc'

    if os.getenv('DBT_TERADATA_SERVER_NAME'):
        hostname=os.getenv('DBT_TERADATA_SERVER_NAME')
    
    if os.getenv('DBT_TERADATA_USERNAME'):
        username=os.getenv('DBT_TERADATA_USERNAME')
    
    if os.getenv('DBT_TERADATA_PASSWORD'):
        password=os.getenv('DBT_TERADATA_PASSWORD')

    with teradatasql.connect ('{"host":"localhost","user":"dbc","password":"dbc"}') as con:
        with con.cursor () as cur:
            cur.execute ("create user test_grants_user1 from dbc as permanent=10000000 BYTES,password=test_grants_user1;")
            cur.execute ("create user test_grants_user2 from dbc as permanent=10000000 BYTES,password=test_grants_user2;")
            cur.execute ("create user test_grants_user3 from dbc as permanent=10000000 BYTES,password=test_grants_user3;")
    
    return {
        'type': 'teradata',
        'threads': 1,
        'host': hostname, #{{os.getenv('HOST'), 'localhost'}},
        'user': username, #{{os.getenv('USER'), 'dbc'}},
        'password': password, #{{os.getenv('PASSWORD'), 'dbc'}},
        'tmode': 'ANSI',
        'log': '0'
    }

@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    unique_schema = f"dbt_test_{prefix}"   
    return unique_schema

@pytest.fixture(scope="class")
def prefix():
    # create a directory name that will be unique per test session
    _randint = random.randint(0, 9999)
    _runtime_timedelta = datetime.utcnow() - datetime(1970, 1, 1, 0, 0, 0)
    _runtime = (int(_runtime_timedelta.total_seconds() * 1e6)) + _runtime_timedelta.microseconds
    prefix = f"{_runtime}{_randint:04}"
    return prefix