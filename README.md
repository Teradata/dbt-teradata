# dbt-teradata

The dbt Teradata adapter lets you use [dbt](https://getdbt.com) with Teradata Vantage.

**_NOTE:_** This adapter is maintained by Teradata. We are accelerating our release cadence. Starting October 1st, 2023, we will release `dbt-teradata` within 4 weeks of a minor release or within 8 weeks of a major release of `dbt-core`.

## Installation

```
pip install dbt-teradata
```
> **Starting from dbt-teradata 1.8.0 and above, dbt-core will not be installed as a dependency. Therefore, you need to explicitly install dbt-core. Ensure you install dbt-core 1.8.0 or above. You can do this with the following command:**
> ```
> pip install dbt-core>=1.8.0
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

At a minimum, you need to specify `host`, `user`, `password`, `schema` (database), `tmode`.

## Python compatibility

| Plugin version | Python 3.6  | Python 3.7  | Python 3.8  | Python 3.9  | Python 3.10 | Python 3.11 | Python 3.12 |
|----------------| ----------- | ----------- | ----------- | ----------- | ----------- |-------------|-------------|
| 0.19.0.x       | ✅          | ✅          | ✅          | ❌          | ❌          | ❌           | ❌ 
| 0.20.0.x       | ✅          | ✅          | ✅          | ✅          | ❌          | ❌           | ❌ 
| 0.21.1.x       | ✅          | ✅          | ✅          | ✅          | ❌          | ❌           | ❌ 
| 1.0.0.x        | ❌           | ✅          | ✅          | ✅          | ❌          | ❌          | ❌   
| 1.1.x.x        | ❌           | ✅          | ✅          | ✅          | ✅          | ❌          | ❌ 
| 1.2.x.x        | ❌           | ✅          | ✅          | ✅          | ✅          | ❌          | ❌  
| 1.3.x.x        | ❌           | ✅          | ✅          | ✅          | ✅          | ❌          | ❌
| 1.4.x.x        | ❌           | ✅          | ✅          | ✅          | ✅          | ✅          | ❌ 
| 1.5.x          | ❌           | ✅          | ✅          | ✅          | ✅          | ✅          | ❌ 
| 1.6.x          | ❌           | ❌          | ✅          | ✅          | ✅          | ✅          | ❌ 
| 1.7.x          | ❌           | ❌          | ✅          | ✅          | ✅          | ✅          | ❌ 
| 1.8.x          | ❌           | ❌          | ✅          | ✅          | ✅          | ✅          | ✅ 


##  dbt dependent packages version compatibility
| dbt-teradata | dbt-core | dbt-teradata-util | dbt-util       |
|--------------|----------|-------------------|----------------|
| 1.2.x        | 1.2.x    | 0.1.0             | 0.9.x or below |
| 1.6.7        | 1.6.7    | 1.1.1             | 1.1.1          |
| 1.7.x        | 1.7.x    | 1.1.1             | 1.1.1          |
| 1.8.x        | 1.8.x    | 1.2.0             | 1.2.0          |

## Optional profile configurations

### Logmech

The logon mechanism for Teradata jobs that dbt executes can be configured with the `logmech` configuration in your Teradata profile. The `logmech` field can be set to: `TD2`, `LDAP`, `BROWSER`, `KRB5`, `TDNEGO`. For more information on authentication options, go to [Teradata Vantage authentication documentation](https://docs.teradata.com/r/8Mw0Cvnkhv1mk1LEFcFLpw/0Ev5SyB6_7ZVHywTP7rHkQ).

> For the initial BROWSER authentication, the browser opens as expected, asking for the credentials. However, for every subsequent connection, a new browser tab opens, displaying the message 'TERADATA BROWSER AUTHENTICATION COMPLETED,' despite using an existing BROWSER session silently. This is the default behavior of the teradatasql driver, and there is no way to avoid this at the present time.


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
`lob_support`           | `"true"`    | quoted boolean | Controls LOB support. Equivalent to the Teradata JDBC Driver `LOB_SUPPORT` connection parameter.
`log`                   | `"0"`       | quoted integer | Controls debug logging. Somewhat equivalent to the Teradata JDBC Driver `LOG` connection parameter. This parameter's behavior is subject to change in the future. This parameter's value is currently defined as an integer in which the 1-bit governs function and method tracing, the 2-bit governs debug logging, the 4-bit governs transmit and receive message hex dumps, and the 8-bit governs timing. Compose the value by adding together 1, 2, 4, and/or 8.
`logdata`               |             | string         | Specifies extra data for the chosen logon authentication method. Equivalent to the Teradata JDBC Driver `LOGDATA` connection parameter.
`logon_timeout`         | `"0"`       | quoted integer | Specifies the logon timeout in seconds. Zero means no timeout.
`logmech`               | `"TD2"`     | string         | Specifies the logon authentication method. Equivalent to the Teradata JDBC Driver `LOGMECH` connection parameter. Possible values are `TD2` (the default), `JWT`, `LDAP`, `BROWSER`, `KRB5` for Kerberos, or `TDNEGO`.
`max_message_body`      | `"2097000"` | quoted integer | Specifies the maximum Response Message size in bytes. Equivalent to the Teradata JDBC Driver `MAX_MESSAGE_BODY` connection parameter.
`partition`             | `"DBC/SQL"` | string         | Specifies the database partition. Equivalent to the Teradata JDBC Driver `PARTITION` connection parameter.
`request_timeout`       |   `"0"`     | quoted integer | Specifies the timeout for executing each SQL request. Zero means no timeout.
`retries`               |   `0`       | integer        | Allows an adapter to automatically try again when the attempt to open a new connection on the database has a transient, infrequent error. This option can be set using the retries configuration. Default value is 0. The default wait period between connection attempts is one second. retry_timeout (seconds) option allows us to adjust this waiting period.
`runstartup`            |  "false"    | quoted boolean | Controls whether the user's STARTUP SQL request is executed after logon. For more information, refer to User STARTUP SQL Request. Equivalent to the Teradata JDBC Driver RUNSTARTUP connection parameter. If retries is set to 3, the adapter will try to establish a new connection three times if an error occurs.
`sessions`              |             | quoted integer | Specifies the number of data transfer connections for FastLoad or FastExport. The default (recommended) lets the database choose the appropriate number of connections. Equivalent to the Teradata JDBC Driver SESSIONS connection parameter.
`sip_support`           | `"true"`    | quoted boolean | Controls whether StatementInfo parcel is used. Equivalent to the Teradata JDBC Driver `SIP_SUPPORT` connection parameter.
`sp_spl`                | `"true"`    | quoted boolean | Controls whether stored procedure source code is saved in the database when a SQL stored procedure is created. Equivalent to the Teradata JDBC Driver SP_SPL connection parameter.
`sslca`                 |             | string         | Specifies the file name of a PEM file that contains Certificate Authority (CA) certificates for use with `sslmode` values `VERIFY-CA` or `VERIFY-FULL`. Equivalent to the Teradata JDBC Driver `SSLCA` connection parameter.
`sslcrc`                | `"ALLOW"`   | string         | Equivalent to the Teradata JDBC Driver SSLCRC connection parameter. Values are case-insensitive.<br/>&bull; ALLOW provides "soft fail" behavior such that communication failures are ignored during certificate revocation checking. <br/>&bull; REQUIRE mandates that certificate revocation checking must succeed.
`sslcapath`             |             | string         | Specifies a directory of PEM files that contain Certificate Authority (CA) certificates for use with `sslmode` values `VERIFY-CA` or `VERIFY-FULL`. Only files with an extension of `.pem` are used. Other files in the specified directory are not used. Equivalent to the Teradata JDBC Driver `SSLCAPATH` connection parameter.
`sslcipher`             |             | string         | Specifies the TLS cipher for HTTPS/TLS connections. Equivalent to the Teradata JDBC Driver `SSLCIPHER` connection parameter.
`sslmode`               | `"PREFER"`  | string         | Specifies the mode for connections to the database. Equivalent to the Teradata JDBC Driver `SSLMODE` connection parameter.<br/>&bull; `DISABLE` disables HTTPS/TLS connections and uses only non-TLS connections.<br/>&bull; `ALLOW` uses non-TLS connections unless the database requires HTTPS/TLS connections.<br/>&bull; `PREFER` uses HTTPS/TLS connections unless the database does not offer HTTPS/TLS connections.<br/>&bull; `REQUIRE` uses only HTTPS/TLS connections.<br/>&bull; `VERIFY-CA` uses only HTTPS/TLS connections and verifies that the server certificate is valid and trusted.<br/>&bull; `VERIFY-FULL` uses only HTTPS/TLS connections, verifies that the server certificate is valid and trusted, and verifies that the server certificate matches the database hostname.
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
            valid_from='valid_from_column',
            history_column_in_target='history_period_column'
    )
    }}
    ```
  `valid_history` incremental strategy requires the following parameters:
  * `valid_from` - Column in the source table of **timestamp** datatype indicating when each record became valid.
  * `history_column_in_target` - Column in the target table of **period** datatype that tracks history.

  

>   The valid_history strategy in dbt-teradata involves several critical steps to ensure the integrity and accuracy of historical data management:
>   * Remove duplicates and conflicting values from the source data:
>     * This step ensures that the data is clean and ready for further processing by eliminating any redundant or conflicting records.
>     * The process of removing duplicates and conflicting values from the source data involves using a ranking mechanism to ensure that only the highest-priority records are retained. This is accomplished using the SQL RANK() function.
>   * Identify and adjust overlapping time slices:
>     * Overlapping time periods in the data are detected and corrected to maintain a consistent and non-overlapping timeline.
>   * Manage records needing to be overwritten or split based on the source and target data:
>     * This involves handling scenarios where records in the source data overlap with or need to replace records in the target data, ensuring that the historical timeline remains accurate.
>   * Utilize the TD_NORMALIZE_MEET function to compact history:
>     * This function helps to normalize and compact the history by merging adjacent time periods, improving the efficiency and performance of the database.
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
  

>   **Important Note**: The target table must already exist before running the model. Ensure that the target table is created and properly structured with the necessary columns, including a column that tracks the history with period datatype, before running a dbt model.


To learn more about dbt incremental strategies please check [the dbt incremental strategy documentation](https://docs.getdbt.com/docs/build/incremental-models#about-incremental_strategy).

### Commands

All dbt commands are supported.

### Custom configurations

#### General

* *Enable view column types in docs* -  Teradata Vantage has a dbscontrol configuration flag called `DisableQVCI` (QVCI - Queryable View Column Index). This flag instructs the database to build `DBC.ColumnsJQV` with view column type definitions. 

    > :information_source: Existing customers, please see [KB0022230](https://support.teradata.com/knowledge?id=kb_article_view&sys_kb_id=d066248b1b0000187361c8415b4bcb48) for more information about enabling QVCI.

  To enable this functionality you need to:
  1. Enable QVCI mode in Vantage. Use `dbscontrol` utility and then restart Teradata. Run these commands as a privileged user on a Teradata node:
      ```bash
      # option 551 is DisableQVCI. Setting it to false enables QVCI.
      dbscontrol << EOF
      M internal 551=false
      W
      EOF

      # restart Teradata
      tpareset -y Enable QVCI
      ```
  2. Instruct `dbt` to use `QVCI` mode. Include the following variable in your `dbt_project.yml`:
      ```yaml
      vars:
        use_qvci: true
      ```
      For example configuration, see `test/catalog/with_qvci/dbt_project.yml`.

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

`Hash` macro needs an `md5` function implementation. Teradata doesn't support `md5` natively. You need to install a User Defined Function (UDF):
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


## Credits

The adapter was originally created by [Doug Beatty](https://github.com/dbeatty10). Teradata took over the adapter in January 2022. We are grateful to Doug for founding the project and accelerating the integration of dbt + Teradata.

## License

The adapter is published using Apache-2.0 License. Please see [the license](LICENSE) for terms and conditions, such as creating derivative work and the support model. 
