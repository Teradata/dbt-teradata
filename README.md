# dbt-teradata

The dbt Teradata adapter lets you use [dbt](https://getdbt.com) with Teradata Vantage.

**_NOTE:_** This adapter is maintained by Teradata. We are accelerating our release cadence. Starting October 1st, 2023, we will release `dbt-teradata` within 4 weeks of a minor release or within 8 weeks of a major release of `dbt-core`.

## Installation

```
pip install dbt-teradata
```
> **Starting from dbt-teradata 1.8.0 and above, dbt-core will not be installed as a dependency. Therefore, you need to explicitly install dbt-core. Ensure you install dbt-core 1.10.0 or above. You can do this with the following command:**
> ```
> pip install dbt-core>=1.10.0
> ```
> Please go through this discussion for more information:
> https://github.com/dbt-labs/dbt-core/discussions/9171

If you are new to dbt on Teradata see [dbt with Teradata Vantage tutorial](https://quickstarts.teradata.com/dbt.html).

**_NOTE:_** If the virtual environment in Python is not activating properly on Windows, you can try running the below command in the command-line interface (CLI) before attempting to activate the virtual environment.

```
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUse
```

## Sample profile

Here is a working example of a `dbt-teradata` profile:

```yaml
my-teradata-db-profile:
  target: dev
  outputs:
    dev:
      type: teradata
      host: localhost
      user: dbc
      password: dbc
      schema: dbt_test
      tmode: ANSI
```

At a minimum, you need to specify `host`, `user`, `password`, `schema` (database).

## Python compatibility

| Plugin version | Python 3.6  | Python 3.7  | Python 3.8 | Python 3.9  | Python 3.10 | Python 3.11 | Python 3.12 | Python 3.13 |
|----------------| ----------- | ----------- | --------- | ----------- | ----------- |-------------|-------------|-------------|
| 0.19.0.x       | ✅          | ✅          | ✅         | ❌          | ❌          | ❌           | ❌         | ❌
| 0.20.0.x       | ✅          | ✅          | ✅         | ✅          | ❌          | ❌           | ❌         | ❌
| 0.21.1.x       | ✅          | ✅          | ✅         | ✅          | ❌          | ❌           | ❌         | ❌
| 1.0.0.x        | ❌           | ✅          | ✅         | ✅          | ❌          | ❌          | ❌         | ❌
| 1.1.x.x        | ❌           | ✅          | ✅         | ✅          | ✅          | ❌          | ❌         | ❌
| 1.2.x.x        | ❌           | ✅          | ✅         | ✅          | ✅          | ❌          | ❌         | ❌
| 1.3.x.x        | ❌           | ✅          | ✅         | ✅          | ✅          | ❌          | ❌         | ❌
| 1.4.x.x        | ❌           | ✅          | ✅         | ✅          | ✅          | ✅          | ❌         | ❌
| 1.5.x          | ❌           | ✅          | ✅         | ✅          | ✅          | ✅          | ❌         | ❌
| 1.6.x          | ❌           | ❌          | ✅         | ✅          | ✅          | ✅          | ❌         | ❌
| 1.7.x          | ❌           | ❌          | ✅         | ✅          | ✅          | ✅          | ❌         | ❌
| 1.8.x          | ❌           | ❌          | ✅         | ✅          | ✅          | ✅          | ✅         | ❌
| 1.8.2          | ❌           | ❌          | ❌         | ✅          | ✅          | ✅          | ✅         | ❌
| 1.8.3          | ❌           | ❌          | ❌         | ✅          | ✅          | ✅          | ✅         | ❌
| 1.9.x          | ❌           | ❌          | ❌         | ✅          | ✅          | ✅          | ✅         | ❌
| 1.10.x         | ❌           | ❌          | ❌         | ❌          | ✅          | ✅          | ✅         | ✅


##  dbt dependent packages version compatibility
| dbt-teradata | dbt-core | dbt-teradata-util | dbt-util       |
|--------------|----------|-------------------|----------------|
| 1.2.x        | 1.2.x    | 0.1.0             | 0.9.x or below |
| 1.6.7        | 1.6.7    | 1.1.1             | 1.1.1          |
| 1.7.x        | 1.7.x    | 1.1.1             | 1.1.1          |
| 1.8.x        | 1.8.x    | 1.2.0             | 1.2.0          |
| 1.8.x        | 1.8.x    | 1.3.0             | 1.3.0          |
| 1.9.x        | 1.9.x    | 1.3.0             | 1.3.0          |
| 1.10.x       | 1.10.x   | 1.3.0             | 1.3.0          |

## Optional profile configurations

### Logmech

The logon mechanism for Teradata jobs that dbt executes can be configured with the `logmech` configuration in your Teradata profile. The `logmech` field can be set to: `TD2`, `LDAP`, `BROWSER`, `KRB5`, `TDNEGO`. For more information on authentication options, go to [Teradata Vantage authentication documentation](https://docs.teradata.com/r/8Mw0Cvnkhv1mk1LEFcFLpw/0Ev5SyB6_7ZVHywTP7rHkQ).

> When running a dbt job with logmech set to "browser", the initial authentication opens a browser window where you must enter your username and password.<br>
After authentication, this window remains open, requiring you to manually switch back to the dbt console.<br>
For every subsequent connection, a new browser tab briefly opens, displaying the message "TERADATA BROWSER AUTHENTICATION COMPLETED," and silently reuses the existing session.<br>
However, the focus stays on the browser window, so you’ll need to manually switch back to the dbt console each time.<br>
This behavior is the default functionality of the teradatasql driver and cannot be avoided at this time.<br>
To prevent session expiration and the need to re-enter credentials, ensure the authentication browser window stays open until the job is complete.

```yaml
my-teradata-db-profile:
  target: dev
  outputs:
    dev:
      type: teradata
      host: <host>
      user: <user>
      password: <password>
      schema: dbt_test
      tmode: ANSI
      logmech: LDAP
```

### Logdata

The logon mechanism for Teradata jobs that dbt executes can be configured with the `logdata` configuration in your Teradata profile. Addtional data like secure token, distinguished Name, or a domain/realm name can be set in your Teradata profile using `logdata`. The `logdata` field can be set to: `JWT`, `LDAP`, `KRB5`, `TDNEGO`. `logdata` is not used with the TD2 mechanism. 

```yaml
my-teradata-db-profile:
  target: dev
  outputs:
    dev:
      type: teradata
      host: <host>
      schema: dbt_test
      tmode: ANSI
      logmech: LDAP
      logdata: 'authcid=username password=password'
      port: <port>
```

For more information on authentication options, go to [Teradata Vantage authentication documentation](https://docs.teradata.com/r/8Mw0Cvnkhv1mk1LEFcFLpw/0Ev5SyB6_7ZVHywTP7rHkQ)

### Stored Password Protection

Stored Password Protection enables an application to provide a connection password in encrypted form to the driver. The plugin supports Stored Password Protection feature through prefix `ENCRYPTED_PASSWORD(` either in `password` connection parameter  or in `logdata` connection parameter.

* `password`

```yaml
my-teradata-db-profile:
  target: dev
  outputs:
    dev:
      type: teradata
      host: <host>
      user: <user>
      password: ENCRYPTED_PASSWORD(file:PasswordEncryptionKeyFileName,file:EncryptedPasswordFileName)
      schema: dbt_test
      tmode: ANSI
      port: <port>
```
* `logdata`

```yaml
my-teradata-db-profile:
  target: dev
  outputs:
    dev:
      type: teradata
      host: <host>
      schema: dbt_test
      tmode: ANSI
      logmech: LDAP
      logdata: 'authcid=username password=ENCRYPTED_PASSWORD(file:PasswordEncryptionKeyFileName,file:EncryptedPasswordFileName)'
      port: <port>
```

For full description of Stored Password Protection see https://github.com/Teradata/python-driver#StoredPasswordProtection.


### Port

If your Teradata database runs on port different than the default (1025), you can specify a custom port in your dbt profile using `port` configuration.

```yaml
my-teradata-db-profile:
  target: dev
  outputs:
    dev:
      type: teradata
      host: <host>
      user: <user>
      password: <password>
      schema: dbt_test
      tmode: ANSI
      port: <port>
```

### Retries

Allows an adapter to automatically try again when the attempt to open a new connection on the database has a transient, infrequent error. This option can be set using the `retries` configuration. Default value is 0. The default wait period between connection attempts is one second. `retry_timeout` (seconds) option allows us to adjust this waiting period.

If `retries` is set to 3, the adapter will try to establish a new connection three times if an error occurs.

```yaml
my-teradata-db-profile:
  target: dev
  outputs:
    dev:
      type: teradata
      host: <host>
      user: <user>
      password: <password>
      schema: dbt_test
      tmode: ANSI
      retries: 3
      retry_timeout: 10
```

### Description of Teradata Profile Fields

The following fields are required:

Parameter               | Default    | Type           | Description
----------------------- |------------| -------------- | ---
`user`                  |            | string         | Specifies the database username. Equivalent to the Teradata JDBC Driver `USER` connection parameter.
`password`              |            | string         | Specifies the database password. Equivalent to the Teradata JDBC Driver `PASSWORD` connection parameter.
`schema`                |            | string         | Specifies the initial database to use after logon, instead of the user's default database.
`tmode`                 | `"ANSI"`   | string         | Specifies the transaction mode. Only `ANSI` mode is currently supported.


The plugin also supports the following optional connection parameters:

Parameter               | Default     | Type           | Description
----------------------- | ----------- | -------------- | ---
`account`               |             | string         | Specifies the database account. Equivalent to the Teradata JDBC Driver `ACCOUNT` connection parameter.
`browser`               |             | string         | Specifies the command to open the browser for Browser Authentication, when logmech is BROWSER. Browser Authentication is supported for Windows and macOS. Equivalent to the Teradata JDBC Driver BROWSER connection parameter.
`browser_tab_timeout`   |   `"5"`     | quoted integer | Specifies the number of seconds to wait before closing the browser tab after Browser Authentication is completed. The default is 5 seconds. The behavior is under the browser's control, and not all browsers support automatic closing of browser tabs.
`browser_timeout`       |   `"180"`   | quoted integer | Specifies the number of seconds that the driver will wait for Browser Authentication to complete. The default is 180 seconds (3 minutes).
`column_name`           | `"false"`   | quoted boolean | Controls the behavior of cursor `.description` sequence `name` items. Equivalent to the Teradata JDBC Driver `COLUMN_NAME` connection parameter. False specifies that a cursor `.description` sequence `name` item provides the AS-clause name if available, or the column name if available, or the column title. True specifies that a cursor `.description` sequence `name` item provides the column name if available, but has no effect when StatementInfo parcel support is unavailable.
`connect_timeout`       |  `"10000"`  | quoted integer | Specifies the timeout in milliseconds for establishing a TCP socket connection. Specify 0 for no timeout. The default is 10 seconds (10000 milliseconds).
`cop`                   | `"true"`    | quoted boolean | Specifies whether COP Discovery is performed. Equivalent to the Teradata JDBC Driver `COP` connection parameter.
`coplast`               | `"false"`   | quoted boolean | Specifies how COP Discovery determines the last COP hostname. Equivalent to the Teradata JDBC Driver `COPLAST` connection parameter. When `coplast` is `false` or omitted, or COP Discovery is turned off, then no DNS lookup occurs for the coplast hostname. When `coplast` is `true`, and COP Discovery is turned on, then a DNS lookup occurs for a coplast hostname.
`port`                  | `"1025"`    | quoted integer | Specifies the database port number. Equivalent to the Teradata JDBC Driver `DBS_PORT` connection parameter.
`encryptdata`           | `"false"`   | quoted boolean | Controls encryption of data exchanged between the driver and the database. Equivalent to the Teradata JDBC Driver `ENCRYPTDATA` connection parameter.
`fake_result_sets`      | `"false"`   | quoted boolean | Controls whether a fake result set containing statement metadata precedes each real result set.
`field_quote`           | `"\""`      | string         | Specifies a single character string used to quote fields in a CSV file.
`field_sep`             | `","`       | string         | Specifies a single character string used to separate fields in a CSV file. Equivalent to the Teradata JDBC Driver `FIELD_SEP` connection parameter.
`host`                  |             | string         | Specifies the database hostname.
`https_port`            | `"443"`     | quoted integer | Specifies the database port number for HTTPS/TLS connections. Equivalent to the Teradata JDBC Driver `HTTPS_PORT` connection parameter.
`http_proxy`            |             | string | Specifies the proxy server URL for HTTP connections to TLS certificate verification CRL and OCSP endpoints. The URL must begin with http:// and must include a colon : and port number.
`http_proxy_user`       |             | string | Specifies the proxy server username for the proxy server identified by the http_proxy parameter. This parameter may only be specified in conjunction with the http_proxy parameter. When this parameter is omitted, no proxy server username is provided to the proxy server identified by the http_proxy parameter.
`http_proxy_password`   |             | string | Specifies the proxy server password for the proxy server identified by the http_proxy parameter. This parameter may only be specified in conjunction with the http_proxy parameter. When this parameter is omitted, no proxy server password is provided to the proxy server identified by the http_proxy parameter.
`https_proxy`           |             | string | Specifies the proxy server URL for HTTPS/TLS connections to the database and to Identity Provider endpoints. The URL must begin with http:// and must include a colon : and port number. The driver connects to the proxy server using a non-TLS HTTP connection, then uses the HTTP CONNECT method to establish an HTTPS/TLS connection to the destination. Equivalent to the Teradata JDBC Driver HTTPS_PROXY connection parameter.
`https_proxy_user`      |             | string | Specifies the proxy server username for the proxy server identified by the https_proxy parameter. This parameter may only be specified in conjunction with the https_proxy parameter. When this parameter is omitted, no proxy server username is provided to the proxy server identified by the https_proxy parameter. Equivalent to the Teradata JDBC Driver HTTPS_PROXY_USER connection parameter.
`https_proxy_password`  |             | string | Specifies the proxy server password for the proxy server identified by the https_proxy parameter. This parameter may only be specified in conjunction with the https_proxy parameter. When this parameter is omitted, no proxy server password is provided to the proxy server identified by the https_proxy parameter. Equivalent to the Teradata JDBC Driver HTTPS_PROXY_PASSWORD connection parameter.
`lob_support`           | `"true"`    | quoted boolean | Controls LOB support. Equivalent to the Teradata JDBC Driver `LOB_SUPPORT` connection parameter.
`log`                   | `"0"`       | quoted integer | Controls debug logging. Somewhat equivalent to the Teradata JDBC Driver `LOG` connection parameter. This parameter's behavior is subject to change in the future. This parameter's value is currently defined as an integer in which the 1-bit governs function and method tracing, the 2-bit governs debug logging, the 4-bit governs transmit and receive message hex dumps, and the 8-bit governs timing. Compose the value by adding together 1, 2, 4, and/or 8.
`logdata`               |             | string         | Specifies extra data for the chosen logon authentication method. Equivalent to the Teradata JDBC Driver `LOGDATA` connection parameter.
`logon_timeout`         | `"0"`       | quoted integer | Specifies the logon timeout in seconds. Zero means no timeout.
`logmech`               | `"TD2"`     | string         | Specifies the logon authentication method. Equivalent to the Teradata JDBC Driver `LOGMECH` connection parameter. Possible values are `TD2` (the default), `JWT`, `LDAP`, `BROWSER`, `KRB5` for Kerberos, or `TDNEGO`.
`max_message_body`      | `"2097000"` | quoted integer | Specifies the maximum Response Message size in bytes. Equivalent to the Teradata JDBC Driver `MAX_MESSAGE_BODY` connection parameter.
`partition`             | `"DBC/SQL"` | string         | Specifies the database partition. Equivalent to the Teradata JDBC Driver `PARTITION` connection parameter.
`proxy_bypass_hosts`    |             | string         | Specifies a matching pattern for hostnames and addresses to bypass the proxy server identified by the http_proxy and/or https_proxy parameter. This parameter may only be specified in conjunction with the http_proxy and/or https_proxy parameter. Separate multiple hostnames and addresses with a vertical bar | character. Specify an asterisk * as a wildcard character. When this parameter is omitted, the default pattern localhost|127.*|[::1] bypasses the proxy server identified by the http_proxy and/or https_proxy parameter for common variations of the loopback address. Equivalent to the Teradata JDBC Driver PROXY_BYPASS_HOSTS connection parameter.
`request_timeout`       |   `"0"`     | quoted integer | Specifies the timeout for executing each SQL request. Zero means no timeout.
`retries`               |   `0`       | integer        | Allows an adapter to automatically try again when the attempt to open a new connection on the database has a transient, infrequent error. This option can be set using the retries configuration. Default value is 0. The default wait period between connection attempts is one second. retry_timeout (seconds) option allows us to adjust this waiting period.
`runstartup`            |  "false"    | quoted boolean | Controls whether the user's STARTUP SQL request is executed after logon. For more information, refer to User STARTUP SQL Request. Equivalent to the Teradata JDBC Driver RUNSTARTUP connection parameter. If retries is set to 3, the adapter will try to establish a new connection three times if an error occurs.
`sessions`              |             | quoted integer | Specifies the number of data transfer connections for FastLoad or FastExport. The default (recommended) lets the database choose the appropriate number of connections. Equivalent to the Teradata JDBC Driver SESSIONS connection parameter.
`sip_support`           | `"true"`    | quoted boolean | Controls whether StatementInfo parcel is used. Equivalent to the Teradata JDBC Driver `SIP_SUPPORT` connection parameter.
`sp_spl`                | `"true"`    | quoted boolean | Controls whether stored procedure source code is saved in the database when a SQL stored procedure is created. Equivalent to the Teradata JDBC Driver SP_SPL connection parameter.
`sslca`                 |             | string         | Specifies the file name of a PEM file that contains Certificate Authority (CA) certificates for use with `sslmode` values `VERIFY-CA` or `VERIFY-FULL`. Equivalent to the Teradata JDBC Driver `SSLCA` connection parameter.
`sslcrc`                | `"ALLOW"`   | string         | Equivalent to the Teradata JDBC Driver SSLCRC connection parameter. Values are case-insensitive.<br/>&bull; ALLOW provides "soft fail" behavior such that communication failures are ignored during certificate revocation checking. <br/>&bull; REQUIRE mandates that certificate revocation checking must succeed.
`sslcrl`                | `"true"`    | quoted boolean | Controls the use of Certificate Revocation List (CRL) for TLS certificate revocation checking for HTTPS/TLS connections. Online Certificate Status Protocol (OCSP) is preferred over CRL, so CRL is used when OSCP is unavailable. Equivalent to the Teradata JDBC Driver SSLCRL connection parameter.
`sslcapath`             |             | string         | Specifies a directory of PEM files that contain Certificate Authority (CA) certificates for use with `sslmode` values `VERIFY-CA` or `VERIFY-FULL`. Only files with an extension of `.pem` are used. Other files in the specified directory are not used. Equivalent to the Teradata JDBC Driver `SSLCAPATH` connection parameter.
`sslcipher`             |             | string         | Specifies the TLS cipher for HTTPS/TLS connections. Equivalent to the Teradata JDBC Driver `SSLCIPHER` connection parameter.
`sslmode`               | `"PREFER"`  | string         | Specifies the mode for connections to the database. Equivalent to the Teradata JDBC Driver `SSLMODE` connection parameter.<br/>&bull; `DISABLE` disables HTTPS/TLS connections and uses only non-TLS connections.<br/>&bull; `ALLOW` uses non-TLS connections unless the database requires HTTPS/TLS connections.<br/>&bull; `PREFER` uses HTTPS/TLS connections unless the database does not offer HTTPS/TLS connections.<br/>&bull; `REQUIRE` uses only HTTPS/TLS connections.<br/>&bull; `VERIFY-CA` uses only HTTPS/TLS connections and verifies that the server certificate is valid and trusted.<br/>&bull; `VERIFY-FULL` uses only HTTPS/TLS connections, verifies that the server certificate is valid and trusted, and verifies that the server certificate matches the database hostname.
`sslocsp`               |  `"true"`   | quoted boolean | Controls the use of Online Certificate Status Protocol (OCSP) for TLS certificate revocation checking for HTTPS/TLS connections. Equivalent to the Teradata JDBC Driver SSLOCSP connection parameter.
`sslprotocol`           | `"TLSv1.2"` | string         | Specifies the TLS protocol for HTTPS/TLS connections. Equivalent to the Teradata JDBC Driver `SSLPROTOCOL` connection parameter.
`teradata_values`       | `"true"`    | quoted boolean | Controls whether `str` or a more specific Python data type is used for certain result set column value types.
`query_band`            | `"org=teradata-internal-telem;appname=dbt;"`    | string | Specifies the Query Band string to be set for each SQL request.

Refer to [connection parameters](https://github.com/Teradata/python-driver#connection-parameters) for the full description of the connection parameters.

## Supported Features

### Materializations

* `view`
* `table`
* `ephemeral`
* `incremental`

#### Incremental Materialization
The following incremental materialization strategies are supported:
* `append` (default)
* `delete+insert`
* `merge`
* `valid_history`
* `microbatch (Beta)`
    ###### IMPORTANT NOTE: Microbatch strategy is in Beta version from dbt's side itself. There are ongoing enhancements and bug fixes from dbt. We strongly advise validating all records or transformations utilizing this strategy to preempt any potential anomalies or errors.


    ###### 'valid_history' incremental materialization strategy (early access)
    This strategy is designed to manage historical data efficiently within a Teradata environment, leveraging dbt features to ensure data quality and optimal resource usage.
    In temporal databases, valid time is crucial for applications like historical reporting, ML training datasets, and forensic analysis.
  ```yaml
    {{
        config(
            materialized='incremental',
            unique_key='id',
            on_schema_change='fail',
            incremental_strategy='valid_history',
            valid_period='valid_period_col',
            use_valid_to_time='no',
    )
    }}
    ```
  `valid_history` incremental strategy requires the following parameters:
  * `unique_key`: The primary key of the model (excluding the valid time components), specified as a column name or list of column names.
  * `valid_period`: Name of the model column indicating the period for which the record is considered to be valid. The datatype must be `PERIOD(DATE)` or `PERIOD(TIMESTAMP)`. 
  * `use_valid_to_time`: Wether the end bound value of the valid period in the input is considered by the strategy when building the valid timeline. Use 'no' if you consider your record to be valid until changed (and supply any value greater to the begin bound for the end bound of the period - a typical convention is `9999-12-31` of ``9999-12-31 23:59:59.999999`). Use 'yes' if you know until when the record is valid (typically this is a correction in the history timeline).

  

>   The valid_history strategy in dbt-teradata involves several critical steps to ensure the integrity and accuracy of historical data management:
>   * Remove duplicates and conflicting values from the source data:
>     * This step ensures that the data is clean and ready for further processing by eliminating any redundant or conflicting records.
>     * The process of removing primary key duplicates (ie. two or more records with the same value for the `unique_key` and BEGIN() bond of the `valid_period` fields) in the dataset produced by the model. If such duplicates exist, the row with the lowest value is retained for all non-primary-key fields (in the order specified in the model) is retained. Full-row duplicates are always de-duplicated.
>   * Identify and adjust overlapping time slices:
>     * Overlapping or adjacent time periods in the data are corrected to maintain a consistent and non-overlapping timeline. To achieve this, the macro adjusts the valid period end bound of a record to align with the begin bound of the next record (if they overlap or are adjacent) within the same `unique_key` group. If `use_valid_to_time = 'yes'`, the valid period end bound provided in the source data is used. Otherwise, a default end date is applied for missing bounds, and adjustments are made accordingly.
>   * Manage records needing to be adjusted, deleted or split based on the source and target data:
>     * This involves handling scenarios where records in the source data overlap with or need to replace records in the target data, ensuring that the historical timeline remains accurate.
>   * Compact history:
>     * Normalize and compact the history by merging records of adjacent time periods withe same value, optimizing database storage and performance. We use the function TD_NORMALIZE_MEET for this purpose.
>   * Delete existing overlapping records from the target table:
>     * Before inserting new or updated records, any existing records in the target table that overlap with the new data are removed to prevent conflicts.
>   * Insert the processed data into the target table:
>     * Finally, the cleaned and adjusted data is inserted into the target table, ensuring that the historical data is up-to-date and accurately reflects the intended timeline.
>     
>     
> These steps collectively ensure that the valid_history strategy effectively manages historical data, maintaining its integrity and accuracy while optimizing performance.

  ```sql
    An illustration demonstrating the source sample data and its corresponding target data:  
  
    -- Source data
        pk |       valid_from          | value_txt1 | value_txt2
        ======================================================================
        1  | 2024-03-01 00:00:00.0000  | A          | x1
        1  | 2024-03-12 00:00:00.0000  | B          | x1
        1  | 2024-03-12 00:00:00.0000  | B          | x2
        1  | 2024-03-25 00:00:00.0000  | A          | x2
        2  | 2024-03-01 00:00:00.0000  | A          | x1
        2  | 2024-03-12 00:00:00.0000  | C          | x1
        2  | 2024-03-12 00:00:00.0000  | D          | x1
        2  | 2024-03-13 00:00:00.0000  | C          | x1
        2  | 2024-03-14 00:00:00.0000  | C          | x1
    
    -- Target data
        pk | valid_period                                                       | value_txt1 | value_txt2
        ===================================================================================================
        1  | PERIOD(TIMESTAMP)[2024-03-01 00:00:00.0, 2024-03-12 00:00:00.0]    | A          | x1
        1  | PERIOD(TIMESTAMP)[2024-03-12 00:00:00.0, 2024-03-25 00:00:00.0]    | B          | x1
        1  | PERIOD(TIMESTAMP)[2024-03-25 00:00:00.0, 9999-12-31 23:59:59.9999] | A          | x2
        2  | PERIOD(TIMESTAMP)[2024-03-01 00:00:00.0, 2024-03-12 00:00:00.0]    | A          | x1
        2  | PERIOD(TIMESTAMP)[2024-03-12 00:00:00.0, 9999-12-31 23:59:59.9999] | C          | x1
  ```
  



To learn more about dbt incremental strategies please check [the dbt incremental strategy documentation](https://docs.getdbt.com/docs/build/incremental-models#about-incremental_strategy).

### Commands

All dbt commands are supported.

### Custom configurations

#### Models

##### Table

The following options apply to table, snapshots and seed materializations.

* `table_kind` - define the table kind. Legal values are `MULTISET` (default for ANSI transaction mode required by `dbt-teradata`) and `SET`, e.g.:
    * in sql materialization definition file:
      ```yaml
      {{
        config(
            materialized="table",
            table_kind="SET"
        )
      }}
      ```
    * in seed configuration:
      ```yaml
      seeds:
        <project-name>:
          table_kind: "SET"
      ```
  For details, see [CREATE TABLE documentation](https://docs.teradata.com/r/76g1CuvvQlYBjb2WPIuk3g/B6Js16DRQVwPDjgJ8rz7hg).
* `table_option` - define table options. Legal values are:
    ```ebnf
    { MAP = map_name [COLOCATE USING colocation_name] |
      [NO] FALLBACK [PROTECTION] |
      WITH JOURNAL TABLE = table_specification |
      [NO] LOG |
      [ NO | DUAL ] [BEFORE] JOURNAL |
      [ NO | DUAL | LOCAL | NOT LOCAL ] AFTER JOURNAL |
      CHECKSUM = { DEFAULT | ON | OFF } |
      FREESPACE = integer [PERCENT] |
      mergeblockratio |
      datablocksize |
      blockcompression |
      isolated_loading
    }
    ```
    where:
    * mergeblockratio:
      ```ebnf
      { DEFAULT MERGEBLOCKRATIO |
        MERGEBLOCKRATIO = integer [PERCENT] |
        NO MERGEBLOCKRATIO
      }
      ```
    * datablocksize:
      ```ebnf
      DATABLOCKSIZE = {
        data_block_size [ BYTES | KBYTES | KILOBYTES ] |
        { MINIMUM | MAXIMUM | DEFAULT } DATABLOCKSIZE
      }
      ```
    * blockcompression:
      ```ebnf
      BLOCKCOMPRESSION = { AUTOTEMP | MANUAL | ALWAYS | NEVER | DEFAULT }
        [, BLOCKCOMPRESSIONALGORITHM = { ZLIB | ELZS_H | DEFAULT } ]
        [, BLOCKCOMPRESSIONLEVEL = { value | DEFAULT } ]
      ```
    * isolated_loading:
      ```ebnf
      WITH [NO] [CONCURRENT] ISOLATED LOADING [ FOR { ALL | INSERT | NONE } ]
      ```

    Examples:
    * in sql materialization definition file:
      ```yaml
      {{
        config(
            materialized="table",
            table_option="NO FALLBACK"
        )
      }}
      ```
      ```yaml
      {{
        config(
            materialized="table",
            table_option="NO FALLBACK, NO JOURNAL"
        )
      }}
      ```
      ```yaml
      {{
        config(
            materialized="table",
            table_option="NO FALLBACK, NO JOURNAL, CHECKSUM = ON,
              NO MERGEBLOCKRATIO,
              WITH CONCURRENT ISOLATED LOADING FOR ALL"
        )
      }}
      ```
    * in seed configuration:
      ```yaml
      seeds:
        <project-name>:
          table_option:"NO FALLBACK"
      ```
      ```yaml
      seeds:
        <project-name>:
          table_option:"NO FALLBACK, NO JOURNAL"
      ```
      ```yaml
      seeds:
        <project-name>:
          table_option: "NO FALLBACK, NO JOURNAL, CHECKSUM = ON,
            NO MERGEBLOCKRATIO,
            WITH CONCURRENT ISOLATED LOADING FOR ALL"
      ```

  For details, see [CREATE TABLE documentation](https://docs.teradata.com/r/76g1CuvvQlYBjb2WPIuk3g/B6Js16DRQVwPDjgJ8rz7hg).
* `with_statistics` - should statistics be copied from the base table, e.g.:
    ```yaml
    {{
      config(
          materialized="table",
          with_statistics="true"
      )
    }}
    ```
    This option is not available for seeds as seeds do not use `CREATE TABLE ... AS` syntax.

    For details, see [CREATE TABLE documentation](https://docs.teradata.com/r/76g1CuvvQlYBjb2WPIuk3g/B6Js16DRQVwPDjgJ8rz7hg).

* `index` - defines table indices:
    ```ebnf
    [UNIQUE] PRIMARY INDEX [index_name] ( index_column_name [,...] ) |
    NO PRIMARY INDEX |
    PRIMARY AMP [INDEX] [index_name] ( index_column_name [,...] ) |
    PARTITION BY { partitioning_level | ( partitioning_level [,...] ) } |
    UNIQUE INDEX [ index_name ] [ ( index_column_name [,...] ) ] [loading] |
    INDEX [index_name] [ALL] ( index_column_name [,...] ) [ordering] [loading]
    [,...]
    ```
    where:
    * partitioning_level:
      ```ebnf
      { partitioning_expression |
        COLUMN [ [NO] AUTO COMPRESS |
        COLUMN [ [NO] AUTO COMPRESS ] [ ALL BUT ] column_partition ]
      } [ ADD constant ]
      ```
    * ordering:
      ```ebnf
      ORDER BY [ VALUES | HASH ] [ ( order_column_name ) ]
      ```
    * loading:
      ```ebnf
      WITH [NO] LOAD IDENTITY
      ```
    e.g.:
    * in sql materialization definition file:
      ```yaml
      {{
        config(
            materialized="table",
            index="UNIQUE PRIMARY INDEX ( GlobalID )"
        )
      }}
      ```
      > :information_source: Note, unlike in `table_option`, there are no commas between index statements!
      ```yaml
      {{
        config(
            materialized="table",
            index="PRIMARY INDEX(id)
            PARTITION BY RANGE_N(create_date
                          BETWEEN DATE '2020-01-01'
                          AND     DATE '2021-01-01'
                          EACH INTERVAL '1' MONTH)"
        )
      }}
      ```
      ```yaml
      {{
        config(
            materialized="table",
            index="PRIMARY INDEX(id)
            PARTITION BY RANGE_N(create_date
                          BETWEEN DATE '2020-01-01'
                          AND     DATE '2021-01-01'
                          EACH INTERVAL '1' MONTH)
            INDEX index_attrA (attrA) WITH LOAD IDENTITY"
        )
      }}
      ```
    * in seed configuration:
      ```yaml
      seeds:
        <project-name>:
          index: "UNIQUE PRIMARY INDEX ( GlobalID )"
      ```
      > :information_source: Note, unlike in `table_option`, there are no commas between index statements!
      ```yaml
      seeds:
        <project-name>:
          index: "PRIMARY INDEX(id)
            PARTITION BY RANGE_N(create_date
                          BETWEEN DATE '2020-01-01'
                          AND     DATE '2021-01-01'
                          EACH INTERVAL '1' MONTH)"
      ```
      ```yaml
      seeds:
        <project-name>:
          index: "PRIMARY INDEX(id)
            PARTITION BY RANGE_N(create_date
                          BETWEEN DATE '2020-01-01'
                          AND     DATE '2021-01-01'
                          EACH INTERVAL '1' MONTH)
            INDEX index_attrA (attrA) WITH LOAD IDENTITY"
      ```

#### Seeds

Seeds, in addition to the above materialization modifiers, have the following options:
* `use_fastload` - use [fastload](https://github.com/Teradata/python-driver#FastLoad) when handling `dbt seed` command. The option will likely speed up loading when your seed files have hundreds of thousands of rows. You can set this seed configuration option in your `project.yml` file, e.g.:
    ```yaml
    seeds:
      <project-name>:
        +use_fastload: true
    ```
#### Snapshots
Snapshots uses the HASHROW function of the Teradata database to generate a unique hash value for the 'dbt_scd_id' column. If you want to use your own hash UDF, there is a configuration option in the snapshot model called 'snapshot_hash_udf', which defaults to HASHROW. You can provide a value like <database_name.hash_udf_name>. If only hash_udf_name is provided, it uses the same schema as the model runs.

for e.g. :
  snapshots/snapshot_example.sql
  ```sql
  {% snapshot snapshot_example %}
  {{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      check_cols=["c2"],
      snapshot_hash_udf='GLOBAL_FUNCTIONS.hash_md5'
    )
  }}
  select * from {{ ref('order_payments') }}
  {% endsnapshot %}
  ```


#### Grants

Grants are supported in dbt-teradata adapter with release version 1.2.0 and above. You can use grants to manage access to the datasets you're producing with dbt. To implement these permissions, define grants as resource configs on each model, seed, or snapshot. Define the default grants that apply to the entire project in your `dbt_project.yml`, and define model-specific grants within each model's SQL or YAML file.

for e.g. :
  models/schema.yml
  ```yaml
  models:
    - name: model_name
      config:
        grants:
          select: ['user_a', 'user_b']
  ```

Another e.g. for adding multiple grants:

  ```yaml
  models:
  - name: model_name
    config:
      materialized: table
      grants:
        select: ["user_b"]
        insert: ["user_c"]
  ```
> :information_source: `copy_grants` is not supported in Teradata.

More on Grants can be found at https://docs.getdbt.com/reference/resource-configs/grants

### Cross DB macros
Starting with release 1.3, some macros were migrated from [teradata-dbt-utils](https://github.com/Teradata/dbt-teradata-utils) dbt package to the connector. See the table below for the macros supported from the connector.

For using cross DB macros, teradata-utils as a macro namespace will not be used, as cross DB macros have been migrated from teradata-utils to Dbt-Teradata.


#### Compatibility

|     Macro Group       |           Macro Name          |         Status        |                                 Comment                                |
|:---------------------:|:-----------------------------:|:---------------------:|:----------------------------------------------------------------------:|
| Cross-database macros | current_timestamp             | :white_check_mark:    | custom macro provided                                                  |
| Cross-database macros | dateadd                       | :white_check_mark:    | custom macro provided                                                  |
| Cross-database macros | datediff                      | :white_check_mark:    | custom macro provided, see [compatibility note](#datediff)             |
| Cross-database macros | split_part                    | :white_check_mark:    | custom macro provided                                                  |
| Cross-database macros | date_trunc                    | :white_check_mark:    | custom macro provided                                                  |
| Cross-database macros | hash                          | :white_check_mark:    | custom macro provided, see [compatibility note](#hash)                 |
| Cross-database macros | replace                       | :white_check_mark:    | custom macro provided                                                  |
| Cross-database macros | type_string                   | :white_check_mark:    | custom macro provided                                                  |
| Cross-database macros | last_day                      | :white_check_mark:    | no customization needed, see [compatibility note](#last_day)           |
| Cross-database macros | width_bucket                  | :white_check_mark:    | no customization
| SQL generators        | generate_series               | :white_check_mark:    | custom macro provided
| SQL generators        | date_spine                    | :white_check_mark:    | no customization


#### examples for cross DB macros
Replace:
{{ dbt.replace("string_text_column", "old_chars", "new_chars") }}
{{ replace('abcgef', 'g', 'd') }}

Date truncate:
{{ dbt.date_trunc("date_part", "date") }}
{{ dbt.date_trunc("DD", "'2018-01-05 12:00:00'") }}

#### <a name="datediff"></a>datediff
`datediff` macro in teradata supports difference between dates. Differece between timestamps is not supported.

#### <a name="hash"></a>hash

`Hash` macro needs an `md5` function implementation. Teradata doesn't support `md5` natively. You need to install a User Defined Function (UDF) and optionally specify `md5_udf` [variable](https://docs.getdbt.com/docs/build/project-variables).
If not specified the code defaults to using `GLOBAL_FUNCTIONS.hash_md5`. See below instructions on how to install the custom UDF:
1. Download the md5 UDF implementation from Teradata (registration required): https://downloads.teradata.com/download/extensibility/md5-message-digest-udf.
1. Unzip the package and go to `src` directory.
1. Start up `bteq` and connect to your database.
1. Create database `GLOBAL_FUNCTIONS` that will host the UDF. You can't change the database name as it's hardcoded in the macro:
    ```sql
    CREATE DATABASE GLOBAL_FUNCTIONS AS PERMANENT = 60e6, SPOOL = 120e6;
    ```
1. Create the UDF. Replace `<CURRENT_USER>` with your current database user:
    ```sql
    GRANT CREATE FUNCTION ON GLOBAL_FUNCTIONS TO <CURRENT_USER>;
    DATABASE GLOBAL_FUNCTIONS;
    .run file = hash_md5.btq
    ```
1. Grant permissions to run the UDF with grant option.
    ```sql
    GRANT EXECUTE FUNCTION ON GLOBAL_FUNCTIONS TO PUBLIC WITH GRANT OPTION;
    ```
   
Instruction on how to add md5_udf variable in dbt_project.yml for custom hash function:
```yaml
vars:
  md5_udf: Custom_database_name.hash_method_function
```

#### <a name="last_day"></a>last_day

`last_day` in `teradata_utils`, unlike the corresponding macro in `dbt_utils`, doesn't support `quarter` datepart.

## Common Teradata-specific tasks
* *collect statistics* - when a table is created or modified significantly, there might be a need to tell Teradata to collect statistics for the optimizer. It can be done using `COLLECT STATISTICS` command. You can perform this step using dbt's `post-hooks`, e.g.:
  ```yaml
  {{ config(
    post_hook=[
      "COLLECT STATISTICS ON  {{ this }} COLUMN (column_1,  column_2  ...);"
      ]
  )}}
  ```
  See [Collecting Statistics documentation](https://docs.teradata.com/r/76g1CuvvQlYBjb2WPIuk3g/RAyUdGfvREwbO9J0DMNpLw) for more information.

## Support for model contracts
Model contracts are supported with dbt-teradata v1.7.1 and onwards.
Constraint support and enforcement in dbt-teradata

| Constraint type |	Support	Platform | enforcement |
|-----------------|------------------|-------------|
| not_null	      | ✅ Supported	 | ✅ Enforced |
| primary_key	  | ✅ Supported	 | ✅ Enforced |
| foreign_key	  | ✅ Supported	 | ✅ Enforced |
| unique	      | ✅ Supported	 | ✅ Enforced |
| check	          | ✅ Supported	 | ✅ Enforced |

To find more on model contracts please follow dbt documentations https://docs.getdbt.com/docs/collaborate/govern/model-contracts

## Support for `dbt-utils` package
`dbt-utils` package is supported through `teradata/teradata_utils` dbt package. The package provides a compatibility layer between `dbt_utils` and `dbt-teradata`. See [teradata_utils](https://hub.getdbt.com/teradata/teradata_utils/latest/) package for install instructions.

## Limitations

### Transaction mode
Both ANSI and TERA modes are now supported in dbt-teradata. TERA mode's support is introduced with dbt-teradata 1.7.1, it is an initial implementation.
###### IMPORTANT NOTE: This is an initial implementation of the TERA transaction mode and may not support some use cases. We strongly advise validating all records or transformations utilizing this mode to preempt any potential anomalies or errors

## Query Band
Query Band in dbt-teradata can be set on three levels:
1. Profiles Level: In profiles.yml file, user can provide query_band as below example
    ```yaml 
    query_band: 'application=dbt;'
   ```

2. Project Level: In dbt_project.yml file, user can provide query_band as below
    ```yaml
     models:
     Project_name:
        +query_band: "app=dbt;model={model};"
   ```
3. Model Level: It can be set on model sql file or model level configuration on yaml files
    ```sql
   {{ config( query_band='sql={model};' ) }}
   ```
User can set query_band at any level or on all levels.
With profiles level query_band, dbt-teradata will set the query_band for first time for the session and subsequently for model and project level query band will be updated with respective configuration.
If a user set some key-value pair with value as '{model}' than internally this '{model}' will be replaced with model name, and it can be useful for telemetry tracking of sql/ dbql logging. 
Let model that user is running be stg_orders
```yaml
     models:
     Project_name:
        +query_band: "app=dbt;model={model};"
   ```
{model} will be replaced with 'stg_orders' in runtime.

If no query_band is set by user, default query_band will come in play that is :
```org=teradata-internal-telem;appname=dbt;```

## Unit Testing
* Unit testing is supported in dbt-teradata, allowing users to write and execute unit tests using the dbt test command.
  * For detailed guidance, refer to the dbt documentation.

  > In Teradata, reusing the same alias across multiple common table expressions (CTEs) or subqueries within a single model is not permitted, as it results in parsing errors; therefore, it is essential to assign unique aliases to each CTE or subquery to ensure proper query execution.

## dbt-external-tables
* [dbt-external-tables](https://github.com/dbt-labs/dbt-external-tables) are supported with dbt-teradata from dbt-teradata v1.9.3 onwards.
* Under the hood, dbt-teradata uses the concept of foreign tables to create tables from external sources. More information can be found [here](https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Definition-Language-Syntax-and-Examples/Table-Statements/CREATE-FOREIGN-TABLE)
* User need to add the dbt-external-tables packages as dependency and can be resolved with `dbt deps` command
```yaml
packages:
  - package: dbt-labs/dbt_external_tables
    version: [">=0.9.0", "<1.0.0"]
```
* User need to add dispatch config for the project to pick the overridden macros from dbt-teradata package
```yaml
dispatch:
  - macro_namespace: dbt_external_tables
    search_order: ['dbt', 'dbt_external_tables']
```
* To define `STOREDAS` and `ROWFORMAT` for in dbt-external tables, one of the below options can be used:
    * user can use the standard dbt-external-tables config `file_format` and `row_format` respectively
    * Or user can just add it in `USING` config as mentioned in the Teradata's [documentation](https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Definition-Language-Syntax-and-Examples/Table-Statements/CREATE-FOREIGN-TABLE/CREATE-FOREIGN-TABLE-Syntax-Elements/USING-Clause)

* For external source, which requires authentication, user needs to create authentication object and pass it in `tbl_properties` as `EXTERNAL SECURITY` object.
  For more information on Authentication object please follow this [link](https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Definition-Language-Syntax-and-Examples/Authorization-Statements-for-External-Routines/CREATE-AUTHORIZATION-and-REPLACE-AUTHORIZATION)

* Sample external sources are provided below as references
```yaml
version: 2
sources:
  - name: teradata_external
    schema: "{{ target.schema }}"
    loader: S3

    tables:
      - name: people_csv_partitioned
        external: 
          location: "/s3/s3.amazonaws.com/dbt-external-tables-testing/csv/"
          file_format: "TEXTFILE"
          row_format: '{"field_delimiter":",","record_delimiter":"\n","character_set":"LATIN"}'
          using: |
            PATHPATTERN  ('$var1/$section/$var3')
          tbl_properties: |
            MAP = TD_MAP1
            ,EXTERNAL SECURITY  MyAuthObj
          partitions:
            - name: section
              data_type: CHAR(1)
        columns:
          - name: id
            data_type: int
          - name: first_name
            data_type: varchar(64)
          - name: last_name
            data_type: varchar(64)
          - name: email
            data_type: varchar(64)
```

```yaml
version: 2
sources:
  - name: teradata_external
    schema: "{{ target.schema }}"
    loader: S3

    tables:
      - name: people_json_partitioned
        external:
          location: '/s3/s3.amazonaws.com/dbt-external-tables-testing/json/'
          using: |
            STOREDAS('TEXTFILE')
            ROWFORMAT('{"record_delimiter":"\n", "character_set":"cs_value"}')
            PATHPATTERN  ('$var1/$section/$var3')
          tbl_properties: |
            MAP = TD_MAP1
            ,EXTERNAL SECURITY  MyAuthObj
          partitions:
            - name: section
              data_type: CHAR(1)
```

## Open Table Format (OTF) support

dbt-teradata can create and read Iceberg / Delta Lake tables via Teradata's native Open Table Format support. OTF tables live in an external object store (S3, Azure, GCS) and are registered with an external catalog (AWS Glue, Unity Catalog, etc.); Teradata accesses them through a pre-created `DATALAKE` object that encapsulates the catalog type, authentication, and object-store path.

### Which OTF flavor is supported

Teradata offers two ways to work with open table formats. **dbt-teradata supports only External OTF (also called JOTF).**

| OTF flavor | DDL emitted | Naming | Supported by dbt-teradata? |
| ---------- | ----------- | ------ | -------------------------- |
| **External OTF (JOTF)** | `CREATE TABLE "<datalake>"."<otf_db>"."<table>" ... AS ...` | 3-part | ✅ **Yes** — this is what the adapter generates |
| **Teradata Managed OTF (MOTF)** | `CREATE MANAGED TABLE <db>.<table>, DATALAKE=<dl> ...` | 2-part | ❌ **No** — not implemented |

> **⚠️ Managed OTF (MOTF) is not supported.** The adapter only emits `CREATE TABLE` with 3-part naming against a pre-created `DATALAKE` object (External OTF). It does not emit `CREATE MANAGED TABLE`, the `DATALAKE=` table clause, `RETENTIONDAYS`, or MOTF primary-index options. External OTF is copy-on-write and does not support `MERGE`, so dbt-teradata only offers append-style writes on OTF tables — `merge`-based upserts would require MOTF, which is not implemented. See [Limitations](#limitations-and-trade-offs) below.

#### What is supported (External OTF / JOTF)

* **Table formats:** Apache Iceberg (verified) and Delta Lake. The adapter defaults to `iceberg` / `parquet`.
* **Materializations:** `table` and `incremental` (with `incremental_strategy='append'` only).
* **Catalogs:** any catalog your `DATALAKE` object is configured for (AWS Glue, Hive, Unity Catalog, REST). Schema evolution is verified on Iceberg (Glue/Hive).
* **Cross-model references:** `ref()` and OTF tables declared as `sources` (3-part naming).
* **Model config:** `partitioned_by`, `sorted_by`, `tblproperties`, `purge_mode`, `alias`, `on_schema_change`, `persist_docs`.

#### What is not supported

* **Managed OTF (MOTF)** — `CREATE MANAGED TABLE` and 2-part managed tables.
* **Incremental strategies other than `append`** — `merge`, `delete+insert`, `valid_history`, `microbatch` raise a compile-time error (External OTF is copy-on-write and has no `MERGE`).
* **`snapshot` materialization** on OTF tables.
* **Model contracts** (`contract.enforced: true`) on the OTF path.
* **Teradata-native table options** — `table_kind`, `table_option`, `with_statistics`, `index` (these describe native Teradata storage and don't apply to Iceberg/Delta).
* **`grants`** on OTF tables (Teradata does not allow `GRANT` on DATALAKE objects).
* **Catalog types other than `datalake`.**

A simple OTF table model looks like this:

```sql
-- models/sales_iceberg.sql
{{ config(
    materialized='table',
    catalog_name='my_otf_catalog',   -- from catalogs.yml
    partitioned_by='YEAR(order_date), country'
) }}
select order_id, customer_id, country, order_date, amount
from {{ ref('stg_orders') }}
```

### Pre-requisites

The following must exist on the Teradata side **before** running dbt:

* A `DATALAKE` object created in Teradata (e.g. via `CREATE DATALAKE my_lake ...`). dbt does not create DATALAKEs.
* A database inside that DATALAKE (the "OTF database") that will hold the OTF tables. dbt does not create this either.
* The dbt user must have permission to `CREATE TABLE` / `DROP TABLE` within the OTF database, and `SELECT` permission to read OTF tables defined elsewhere.

Refer to the Teradata documentation for `CREATE DATALAKE` syntax and the specific permissions required for your catalog backend.

### Configuration

Register the catalog integration in a `catalogs.yml` file at your dbt project root:

```yaml
catalogs:
  - name: my_otf_catalog
    active_write_integration: td_datalake
    write_integrations:
      - name: td_datalake
        catalog_type: datalake
        adapter_properties:
          datalake_name: my_lake        # the pre-created DATALAKE object
          otf_database: my_otf_db       # the pre-created OTF database within it
```

`catalog_type` must be `datalake`. `datalake_name` and `otf_database` are both required and validated at integration registration time.

Reference the catalog from a model via `catalog_name`:

```sql
-- models/sales_iceberg.sql
{{ config(
    materialized='table',
    catalog_name='my_otf_catalog',
    partitioned_by='YEAR(order_date), country',
    sorted_by='customer_id ASC',
    tblproperties="'write.format.default'='parquet', 'gc.enabled'='true'",
    purge_mode='NO PURGE'
) }}
select
    order_id,
    customer_id,
    country,
    order_date,
    amount
from {{ ref('stg_orders') }}
```

### Naming conventions: 2-part vs 3-part

Teradata's native objects use **2-part** naming (`database.object`); in dbt-teradata, the `database` field is unused and the `schema` field carries the Teradata database name. OTF tables are the **only** Teradata objects that use **3-part** naming (`"<datalake>"."<otf_database>"."<table>"`).

For OTF tables, dbt-teradata maps:

| dbt field    | Teradata concept                  |
| ------------ | --------------------------------- |
| `database`   | DATALAKE name (quoted)            |
| `schema`     | OTF database name (quoted)        |
| `identifier` | OTF table name (quoted)           |

When you set `catalog_name` on a model, dbt-teradata pulls `database` and `schema` from the registered catalog integration automatically. For an OTF table defined as a **source** (where there is no `catalog_name` model config), declare the `database` and `schema` explicitly in `sources.yml` — the adapter detects the 3-part shape (database ≠ schema) and renders it correctly:

```yaml
version: 2
sources:
  - name: customer_otf
    database: my_lake          # DATALAKE name
    schema: my_otf_db          # OTF database name
    tables:
      - name: customer_iceberg
```

A `ref()` from another model then compiles to `"my_lake"."my_otf_db"."customer_iceberg"`.

> **⚠️ Important: `database` and `schema` must be different values in `sources.yml`.**
>
> The adapter uses the heuristic `database ≠ schema` to auto-detect that a source is an OTF table and should use 3-part naming. If your DATALAKE object and OTF database happen to share the same name (e.g. both are `my_lake`), the adapter will treat the source as a regular 2-part Teradata relation and generate incorrect SQL.
>
> To avoid this, ensure that your DATALAKE name and OTF database name are always different. This is only a constraint for the `sources.yml` path — models that use `catalog_name` in their config are not affected, because OTF detection is explicit rather than heuristic.

### Supported model config options

| Option                 | Type    | Description                                                                                                |
| ---------------------- | ------- | ---------------------------------------------------------------------------------------------------------- |
| `catalog_name`         | string  | Name of the catalog integration from `catalogs.yml`. Required to mark a model as OTF.                      |
| `partitioned_by`       | string  | Iceberg/Delta partition expression, e.g. `'YEAR(dt), country'`.                                            |
| `sorted_by`            | string  | Sort order, e.g. `'id ASC'`.                                                                               |
| `tblproperties`        | string  | Iceberg/Delta table properties, e.g. `"'gc.enabled'='true'"`.                                              |
| `purge_mode`           | string  | DROP behavior. `'NO PURGE'` (default; removes catalog entry only) or `'PURGE ALL'` (also deletes data files on the object store). Case-insensitive. |
| `incremental_strategy` | string  | For `materialized='incremental'` only. **Only `'append'` is supported** on OTF (see [Incremental materialization](#incremental-materialization-otf)). |
| `on_schema_change`     | string  | For `materialized='incremental'` only. `'ignore'` (default), `'fail'`, `'append_new_columns'`, or `'sync_all_columns'` (best-effort on OTF — see below). |
| `alias`                | string  | Overrides the physical OTF table name in the catalog. The OTF object is created under the alias; the model file name is not used. Works for both `table` and `incremental` OTF models. |

`persist_docs` and standard dbt cache management work on OTF models the same way they do on native tables. **`grants` is not supported on OTF tables** — Teradata does not allow `GRANT` on DATALAKE objects (access control is managed via AUTHORIZATION objects and external IAM/OAuth policies). Setting `grants` on an OTF model emits a warning and is otherwise ignored.

### Incremental materialization (OTF)

OTF tables can be materialized incrementally with `materialized='incremental'` and a `catalog_name`:

```sql
-- models/orders_otf_incremental.sql
{{ config(
    materialized='incremental',
    catalog_name='my_otf_catalog',
    incremental_strategy='append',
    partitioned_by='YEAR(order_date)',
    on_schema_change='append_new_columns'
) }}
select order_id, customer_id, order_date, amount
from {{ ref('stg_orders') }}
{% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
{% endif %}
```

How it runs:

* **First run** creates the OTF table (`CREATE TABLE ... AS ... WITH DATA`).
* **Subsequent runs** load new rows into a regular Teradata staging table, then `INSERT ... SELECT` into the OTF table (positional insert — OTF does not accept a target column list).
* **`--full-refresh`** drops and re-creates the table from scratch.
* Existence is detected by probing the 3-part name with `SELECT ... SAMPLE 0` (OTF tables are not registered in `DBC.TablesV`/`DBC.ColumnsV` under the dbt schema), treating Teradata errors **7825** and **6321** ("OTF table does not exist") as "not found".

> **Only `incremental_strategy='append'` is supported on OTF.** `merge`, `delete+insert`, `valid_history`, and `microbatch` raise a compile-time error. Teradata External OTF does not support `MERGE` and is copy-on-write only, so the upsert-style strategies cannot be honored. Use `append` (optionally with an `is_incremental()` filter to bound the rows appended).

#### `on_schema_change` on OTF

dbt's [`on_schema_change`](https://docs.getdbt.com/docs/build/incremental-models#what-if-the-columns-of-my-incremental-model-change) is supported on OTF incremental models with these values:

| Value | OTF behavior |
| ----- | ------------ |
| `ignore` (default) | No schema reconciliation. The append assumes the source and the existing OTF table have the same columns in the same order. |
| `fail` | Compares the incoming (source) columns with the existing OTF columns and raises a clear error if any column was added or removed. |
| `append_new_columns` | For each column present in the source but not yet in the OTF table, issues a separate `ALTER TABLE ... ADD <col> <type>`; the new columns are added at the end of the table. Pre-existing rows get `NULL` for the new columns; rows inserted on this run carry the new values. The `INSERT` is reordered to match the resulting OTF column layout. |
| `sync_all_columns` | **Best-effort on OTF.** Makes the OTF table match the source: **adds** new columns, **drops** columns no longer in the source (destructive), and applies **type changes** OTF/Iceberg permits (e.g. `int → bigint`, decimal precision widening). A type change OTF cannot apply in place raises a clear error directing you to `--full-refresh`. |

**Limitations of `on_schema_change` on OTF:**

* **`sync_all_columns` is best-effort, and type changes are limited.** Type comparison is done at OTF/Iceberg granularity (via `HELP TABLE`'s `OTF Type`), so `VARCHAR` length and `SMALLINT`-vs-`INTEGER` differences are **not** treated as changes (OTF doesn't preserve them). Only OTF-permitted promotions are applied in place — verified: `int → bigint` ✅ and decimal **precision** widening (same scale) ✅; scale changes, narrowing, and cross-family changes (e.g. `decimal → string`) raise a clear error → use `--full-refresh`. `sync_all_columns` also performs **destructive, irreversible column drops** (OTF has no rollback). If you only ever add columns, prefer `append_new_columns`.
* **`append_new_columns` is additive only.** New source columns are added; columns removed from the source are **kept** on the OTF table (and back-filled with `NULL` for subsequent rows). Existing column **types are never changed**.
* **Column *order* in the model `SELECT` is handled automatically under `append_new_columns`.** OTF inserts are positional (no target column list), but `append_new_columns` realigns the `INSERT ... SELECT` to the table's column layout *by name*, so you do **not** need to place new columns at the end of the `SELECT`, and reordering existing columns is safe. (`ALTER ... ADD` does physically append new columns to the end of the table; the realignment is what keeps the data correct.) Under `on_schema_change='ignore'`, by contrast, **no realignment happens** — the model `SELECT` must produce columns in the same order as the existing OTF table.
* **Each schema change is a separate `ALTER` statement.** OTF cannot combine multiple alter operations into one statement, and External OTF does not allow multi-statement requests, so `N` new columns produce `N` separate `ALTER TABLE ... ADD` statements. There is no rollback if one of them fails midway.
* **Catalog/format support varies.** Schema evolution is verified on **Iceberg** (AWS Glue / Hive). **Unity Catalog does not support schema evolution at all** — any `ALTER` (including `append_new_columns`) will fail at the database. **Delta Lake** may require `delta.columnMapping.mode='name'` for column changes. On unsupported catalogs the `ALTER` surfaces the underlying Teradata error.

To apply a schema change that neither `append_new_columns` nor `sync_all_columns` can do in place (e.g. a `VARCHAR`→numeric change, a decimal scale change, or a type narrowing), run the model with `--full-refresh`.

#### `alias` on incremental OTF models

The `alias` config sets the physical OTF table name in the catalog (the model file name is not used). It works for both `table` and `incremental` OTF models — for incremental models, both the initial CREATE and all subsequent INSERT operations target the alias-named OTF object:

```sql
{{ config(
    materialized='incremental',
    catalog_name='my_otf_catalog',
    incremental_strategy='append',
    alias='orders_iceberg'          -- physical OTF table name
) }}
```

### Error 7825 / 6321 suppression

Teradata raises error **7825** ("OTF table not found in external catalog") — or **6321** ("OTF Error: Table does not exist") on newer OTF engines (e.g. 20.0.0.61) — when a `DROP TABLE` (or existence probe) targets an OTF table that no longer exists in the external catalog (e.g. Glue). dbt-teradata treats both codes the same way it treats native errors 3807/3853/3854 — suppressed under `IF EXISTS` semantics — so re-running a dbt project after an OTF table has been deleted externally does not fail, and first-run incremental existence checks correctly detect a missing table.

### Limitations and trade-offs

* **Only External OTF (JOTF) is supported; Managed OTF (MOTF) is not.** The adapter emits `CREATE TABLE` against a pre-created `DATALAKE` (3-part naming). It does not emit `CREATE MANAGED TABLE`. External OTF is copy-on-write and has no `MERGE`, so dbt-teradata only performs append-style writes (`incremental_strategy='append'`); `merge`-based upserts would require MOTF.
* **Non-atomic re-materialization.** OTF tables use 3-part naming that cannot be renamed via standard DDL, so the adapter cannot use the build-tmp-then-rename pattern that protects native tables on a failed CREATE. An OTF model is dropped before it is re-created — if the CREATE fails, the table is gone. Plan for `--full-refresh` workflows accordingly.
* **Model contracts are not supported on the OTF path.** Setting `contract.enforced: true` together with `catalog_name` raises a compile-time error.
* **Teradata-native table options are not supported.** Setting any of `table_kind`, `table_option`, `with_statistics`, or `index` together with `catalog_name` raises a compile-time error — these options describe native Teradata table storage and do not apply to Iceberg/Delta tables.
* **Only `catalog_type: datalake` is supported.** Other catalog types are rejected with a compile-time error.
* **Incremental: only the `append` strategy is supported.** `merge`/`delete+insert`/`valid_history`/`microbatch` raise compile-time errors. All four `on_schema_change` values are supported, with `sync_all_columns` being best-effort (limited type changes). See [Incremental materialization (OTF)](#incremental-materialization-otf).
* **OTF cannot be used with the `snapshot` materialization.** Setting `catalog_name` on a snapshot raises a compile-time error (snapshots require update/merge semantics OTF does not provide).

## temporary_metadata_generation_schema (earlier fallback_schema)
dbt-teradata internally created temporary tables to fetch the metadata of views for manifest and catalog creation. 
In case if user does not have permission to create tables on the schema they are working on, they can define a temporary_metadata_generation_schema(to which they have proper create and drop privileges) in dbt_project.yml as variable.
```yaml
     vars:
        temporary_metadata_generation_schema: <schema-name>
   ```

## Credits

The adapter was originally created by [Doug Beatty](https://github.com/dbeatty10). Teradata took over the adapter in January 2022. We are grateful to Doug for founding the project and accelerating the integration of dbt + Teradata.

## License

The adapter is published using Apache-2.0 License. Please see [the license](LICENSE) for terms and conditions, such as creating derivative work and the support model. 

