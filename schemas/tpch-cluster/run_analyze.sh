#!/bin/bash

set -e
set -x

db=$1

function analyze {
    NODE=$1
    DB=$2
    echo "ANALYZE $NODE $DB $table"
    psql -At -U postgres -h $NODE -d $DB -c "SELECT table_name FROM information_schema.tables WHERE table_schema='public' and table_name NOT LIKE '%prt%'" | xargs -n 1 -P 16 -I{} bash -c "psql -U postgres -h $NODE -d $DB -c 'ANALYZE VERBOSE {}'"
}

# nodes=(ovh-node-3 ovh-node-4 ovh-node-5 ovh-node-6 ovh-node-7 ovh-node-8)
nodes=(ovh-node-1)
for NODE in "${nodes[@]}"; do
    analyze $NODE $DB &
done
wait
