# dbt-teradata

This plugin ports [dbt](https://getdbt.com) functionality to Teradata Vantage.

## Sample profile

Here is what a typical `dbt-teradata` profile looks like:

<File name='~/.dbt/profiles.yml'>

```yaml
my-teradata-db:
  target: dev
  outputs:
    dev:
      type: teradata
      [<optional_config>](#optional-configurations): <value>
```
</File>

TODO: cover mandatory settings
## Optional configurations

TODO: cover optional configurations

