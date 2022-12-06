# Testing dbt-teradata

## Overview

### Setup Teradata server
Use one of the following methods to setup a test server:
- [Run Vantage Express on VirtualBox](https://quickstarts.teradata.com/docs/17.10/getting.started.vbox.html)
- [Run Vantage Express on VMware](https://quickstarts.teradata.com/docs/17.10/getting.started.vmware.html)

### Install test dependencies
```shell
python3 -m venv env
source env/bin/activate
pip install -r requirements_dev.txt
```

Here are the steps to run the functional tests:
1. set the environment variables related to the system cofiguration for e.g :
   export DBT_TERADATA_SERVER_NAME='sdt****.labs.teradata.com or server_ip'
   export DBT_TERADATA_USERNAME='***'
   export DBT_TERADATA_PASSWORD='***'
   
   If there are no environment variables provided then the default values will be picked which could be found in conftest.py file.
   
2. Run the test with python pytest command, for e.g  
   python -m pytest .\tests\functional\adapter\test_basic.py                                         