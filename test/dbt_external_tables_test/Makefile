test-teradata:
	dbt deps
	dbt seed --full-refresh
	dbt run-operation prep_external
	dbt run-operation dbt_external_tables.stage_external_sources --vars 'ext_full_refresh: true'
	dbt run-operation dbt_external_tables.stage_external_sources
	dbt test

test-all: test-teradata
	echo "Completed successfully"