## dbt-teradata 1.4.0.0

### Features
* Added python 3.11 support.
* Added predicates for delete+insert strategy.
* Replaced deprecated exception functions.
### Fixes

### Docs
* With python 3.7.17, there is an issue that prompts up :-
    ImportError: "No module named '_bz2'" 
    This is particulary with python 3.7.17 only, if possible try to upgrade or downgrade the python version, issue will be resolved.

### Under the hood
* Consolidated timestamp macros under a single timestamps.py file and added testcases.
* Added functional testcases for Unique keys in Incremental models.
* Added tests for query comments.