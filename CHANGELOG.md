## dbt-teradata 1.0.0a

### Features
* dbt-teradata is now fully compatible with Python 3.12.
* Added support for browser-based Single Sign-On (SSO) authentication when connecting to SSO-enabled Teradata databases. Please refer to the README for more details.

### Fixes
* Resolved an issue with the 'show' command.
  * https://github.com/dbt-labs/dbt-adapters/pull/249
* Fixed a TypeError in TeradataAdapter.get_catalog() where it incorrectly took three arguments instead of two.
  * https://github.com/Teradata/dbt-teradata/issues/180
* Corrected exception handling for failed database connections.
  * https://github.com/Teradata/dbt-teradata/issues/183

### Docs
* Updated the README to include additional Teradata profile fields and threads options.

### Under the hood
* Expanded test coverage for snapshots.
* Verified the compatibility of dbt's threads feature with dbt-teradata.
