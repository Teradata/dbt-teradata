#!/usr/bin/env bash
set -e

cat << EOF > /tmp/sampledata.csv
animal,superpower,magic_index
unicorn,magic,100
pegasus,flying,500
EOF

# generate a file with 1M rows
for i in {1..20}; do
  cat /tmp/sampledata.csv /tmp/sampledata.csv > /tmp/sampledata.tmp && mv /tmp/sampledata.tmp /tmp/sampledata.csv
done

mv /tmp/sampledata.csv seeds/

# run dbt seed with 3m budget
timeout 5m dbt -d seed --target dbt_perf_test
