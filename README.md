# dbt-teradata

This plugin ports [dbt](https://getdbt.com) functionality to Teradata Vantage.

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
## Optional configurations

### Logmech

The logon mechanism for Teradata jobs that dbt executes can be configured with the `logmech` configuration in your Teradata profile. The `logmech` field can be set to: `TD2`, `LDAP`, `KRB5`, `TDNEGO`. For more information on authentication options, go to [Teradata Vantage authentication documentation](hhttps://docs.teradata.com/r/8Mw0Cvnkhv1mk1LEFcFLpw/0Ev5SyB6_7ZVHywTP7rHkQ).

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

* *Enable view column types in docs* -  Teradata Vantage has a dbscontrol configuration flag called `DisableQVCI`. This flag instructs the database to create `DBC.ColumnsJQV` with view column type definitions. To enable this functionality you need to:
  1. Enable QVCI mode in Vantage. Use `dbscontrol` utility and then restart Teradata. Run these commands as a privileged user on a Teradata node:
      ```bash
      # option 551 is DisableQVCI. Setting it to false enables QVCI.
      dbscontrol << EOF
      M internal 551=false
      W
      EOF

      # restart Teradata
      tpareset –y Enable QVCI
      ```
  2. Instruct `dbt` to use `QVCI` mode. Include the following variable in your `dbt_project.yml`:
      ```yaml
      vars:
        use_qvci: true
      ```
      For example configuration, see `test/catalog/with_qvci/dbt_project.yml`.

#### Seeds

* `use_fastload` configuration will instruct the plugin to use [fastload](https://github.com/Teradata/python-driver#FastLoad) when handling `dbt seed` command. You can set this seed configuration option in your `project.yml` file, e.g.:
    ```yaml
    seeds:
      <project-name>:
        +use_fastload: true
    ```

## Limitations

### Transaction mode
Only ANSI transaction mode is supported.

