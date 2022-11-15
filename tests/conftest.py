import pytest
import os
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
    
    return {
        'type': 'teradata',
        'threads': 1,
        'host': hostname, #{{os.getenv('HOST'), 'localhost'}},
        'user': username, #{{os.getenv('USER'), 'dbc'}},
        'password': password, #{{os.getenv('PASSWORD'), 'dbc'}},
        'schema': "dbt_test_{{ var('_dbt_random_suffix') }}"
    }