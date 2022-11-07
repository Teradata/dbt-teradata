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
1. Set up the server in file conftest.py, provide server configurations like
   hostname, username, password etc.
2. Run the test with python pytest command, for e.g  
   python -m pytest .\tests\functional\adapter\test_basic.py                                         