#!/usr/bin/env bash

set -e

###################################################################################################
# copied from
# https://unix.stackexchange.com/questions/65618/bash-script-wait-for-processes-and-get-return-code
declare -A JOBS

## run command in the background
background() {
  eval $1 & JOBS[$!]="$1"
}

## check exit status of each job
## preserve exit status in ${JOBS}
## returns 1 if any job failed
reap() {
  local cmd
  local status=0
  for pid in ${!JOBS[@]}; do
    cmd=${JOBS[${pid}]}
    wait ${pid} ; JOBS[${pid}]=$?
    if [[ ${JOBS[${pid}]} -ne 0 ]]; then
      status=${JOBS[${pid}]}
      echo -e "[${pid}] Exited with status: ${status}\n${cmd}"
    fi
  done
  return ${status}
}
###################################################################################################

createRegularEnv() {
  local RESULT=$(curl -s -L 'https://api.clearscape.teradata.com/environments' \
  -H "Authorization: Bearer $CSAE_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-raw "{
      \"name\": \"$CSAE_ENV_NAME\",
      \"region\": \"us-central\",
      \"password\": \"$CSAE_ENV_PASSWORD\",
      \"startupScript\": \"#!/bin/bash\n\ndbscontrol << EOF\nm g 53=P\nm i=true\nm i 179=true\nm i 178=true\nm i=false\nm g 55=true\nm g 57=2\nW\nEOF\n\"
  }")
  local TERADATA_SERVER_NAME=$(echo $RESULT | jq -r '.dnsName')
  echo "teradata-server-name=$TERADATA_SERVER_NAME" >> $GITHUB_OUTPUT
}

background createRegularEnv

reap
