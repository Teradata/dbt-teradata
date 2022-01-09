#!/usr/bin/env bash
set -e

dbt -d seed --target dbt_catalog_test
dbt -d run --target dbt_catalog_test
dbt -d docs generate --target dbt_catalog_test


assertJsonElementEquals() {
  VALUE_IN_JSON=`cat target/catalog.json| jq -r $2`
  if [ "$1" == "$VALUE_IN_JSON" ]; then
    echo "Assert successful for '$1' and '$2'"
    return
  else
    echo "Assert failed for $2. Was $VALUE_IN_JSON but expected $1"
    exit 1
  fi
}

# assert on target/catalog.json
assertJsonElementEquals CHARACTER '.nodes["seed.teradata.test_table"].columns.animal.type'
assertJsonElementEquals CHARACTER '.nodes["seed.teradata.test_table"].columns.superpower.type'
assertJsonElementEquals INTEGER '.nodes["seed.teradata.test_table"].columns.magic_index.type'

assertJsonElementEquals CHARACTER '.nodes["model.teradata.table_from_source"].columns.animal.type'
assertJsonElementEquals CHARACTER '.nodes["model.teradata.table_from_source"].columns.superpower.type'
assertJsonElementEquals INTEGER '.nodes["model.teradata.table_from_source"].columns.magic_index.type'

assertJsonElementEquals CHARACTER '.nodes["model.teradata.view_from_source"].columns.animal.type'
assertJsonElementEquals CHARACTER '.nodes["model.teradata.view_from_source"].columns.superpower.type'
assertJsonElementEquals INTEGER '.nodes["model.teradata.view_from_source"].columns.magic_index.type'

