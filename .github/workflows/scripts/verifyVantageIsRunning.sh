#!/usr/bin/env bash

# add bteq to path
export PATH=$PATH:"/Users/runner/Library/Application Support/teradata/client/17.10/bin/"

# prepare bteq test script
cat << EOF > /tmp/test.bteq
.SET EXITONDELAY ON MAXREQTIME 20
.logon 127.0.0.1/dbc,dbc
select current_time;
.logoff
EOF

n=1
until [ "$n" -ge 10 ]
do
  echo "Trying to connect to Vantage Express. Attempt $n. This might take a minute."
  bteq < /tmp/test.bteq && break
  n=$((n+1))
  sshpass -p root ssh -o StrictHostKeyChecking=no -p 4422 root@localhost 'tail -200 /var/log/messages' || true
  echo "Waiting 10 seconds before the next attempt."
  sleep 10
done
