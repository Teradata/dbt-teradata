# dbt-teradata

This plugin ports [dbt](https://getdbt.com) functionality to Teradata Vantage.

## Sample profile

Here is a working example of a `dbt-teradata`:

<File name='~/.dbt/profiles.yml'>

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
</File>

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

## Supported Features

### Materializations

* `view`
* `table`
* `ephemeral`
* `incremental`

### Commands

All, apart from `source` and `snapshot`.

## Limitations

### Connection configuration

The following Teradata configuration options are currently not supported by the adapter: `account`, `cop`, `coplast`, `encryptdata`, `lob_support`, `log`, `logdata`, `max_message_body`, `partition`, `sip_support`.

### Transaction mode
Only ANSI transaction mode is supported.

### Dbt features

Snapshots are currently not supported.
