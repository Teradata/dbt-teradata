# dbt-teradata

This plugin ports [dbt](https://getdbt.com) functionality to Teradata Vantage.

## Installation

```
pip install dbt-teradata
```

If you are new to dbt on Teradata see [dbt with Teradata Vantage tutorial](https://quickstarts.teradata.com/docs/17.10/dbt.html).

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

| Plugin version | Python 3.6  | Python 3.7  | Python 3.8  | Python 3.9  | Python 3.10 |
| -------------- | ----------- | ----------- | ----------- | ----------- | ----------- |
| 0.19.0.x           | ✅          | ✅          | ✅          | ❌          | ❌          |
| 0.20.0.x           | ✅          | ✅          | ✅          | ✅          | ❌          |
| 0.21.1.x           | ✅          | ✅          | ✅          | ✅          | ❌          |
| 1.0.0.x           | ❌           | ✅          | ✅          | ✅          | ❌          |
|1.1.0.x            | ❌           | ✅          | ✅          | ✅          | ✅          |
|1.2.0.x            | ❌           | ✅          | ✅          | ✅          | ✅          |


##  dbt dependent packages version compatibility
| dbt-teradta |  dbt-core  | dbt-teradata-util |  dbt-util      |
|-------------|------------|-------------------|----------------|
| 1.2.x       | 1.2.x      | 0.1.0             | 0.9.x or below |

## Optional profile configurations

### Logmech

The logon mechanism for Teradata jobs that dbt executes can be configured with the `logmech` configuration in your Teradata profile. The `logmech` field can be set to: `TD2`, `LDAP`, `KRB5`, `TDNEGO`. For more information on authentication options, go to [Teradata Vantage authentication documentation](https://docs.teradata.com/r/8Mw0Cvnkhv1mk1LEFcFLpw/0Ev5SyB6_7ZVHywTP7rHkQ).

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

### Other Teradata connection parameters

The plugin also supports the following Teradata connection parameters:
* account
* column_name
* cop
* coplast
* encryptdata
* fake_result_sets
* field_quote
* field_sep
* lob_support
* log
* logdata
* max_message_body
* partition
* sip_support
* teradata_values

For full description of the connection parameters see https://github.com/Teradata/python-driver#connection-parameters.

## Supported Features

### Materializations

* `view`
* `table`
* `ephemeral`
* `incremental`

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

#### Grants

Grants are supported in dbt-teradata adapter with release version 1.2.0 and above
You can manage access to the datasets you're producing with dbt by using grants. To implement these permissions, define grants as resource configs on each model, seed, or snapshot. Define the default grants that apply to the entire project in your ```dbt_project.yml```, and define model-specific grants within each model's SQL or YAML file.

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

copy_grants is not supported in Teradata.
More on Grants can be found over https://docs.getdbt.com/reference/resource-configs/grants


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

## Support for `dbt-utils` package
`dbt-utils` package is supported through `teradata/teradata_utils` dbt package. The package provides a compatibility layer between `dbt_utils` and `dbt-teradata`. See [teradata_utils](https://hub.getdbt.com/teradata/teradata_utils/latest/) package for install instructions.
## Limitations

### Transaction mode
Only ANSI transaction mode is supported.

## Credits

The adapter was originally created by [Doug Beatty](https://github.com/dbeatty10). Teradata took over the adapter in January 2022. We are grateful to Doug for founding the project and accelerating the integration of dbt + Teradata.

## License

The adapter is published using Apache-2.0 License. Please see [the license](LICENSE) for terms and conditions, such as creating derivative work and the support model. 
