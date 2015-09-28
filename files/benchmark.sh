#!/bin/bash

set -e -u

# import settings
GRACE_PERIOD_IN_SECONDS_FOR_COMPACTIONS_TO_SETTLE="3600"
NUM_PARTITIONS="1024000"
ROWS_PER_PARTITION="1000"

# fetch settings
NUM_FETCH_PROCESSES="32"
NUM_FETCH_QUERIES="1000000"


run_benchmark() {
    CASSANDRA="$1"
    RESULT_DIR="results/$CASSANDRA"
    mkdir -p "$RESULT_DIR"

    echo "Deleting /var/lib/cassandra/*/*"
    rm -rf /var/lib/cassandra/*/*

    echo "Starting Cassandra (`date`)"
    ./"$CASSANDRA"/bin/cassandra -f > "$RESULT_DIR"/cassandra.out 2>&1 &
    CASSANDRA_PID="$!"
    sleep 15

    ./"$CASSANDRA"/bin/cqlsh --execute "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    ./"$CASSANDRA"/bin/cqlsh --keyspace ks --execute "CREATE TABLE tbl (a INT, b INT, c INT, d INT, PRIMARY KEY(a, b)) WITH compaction = { 'class':'LeveledCompactionStrategy' }"

    echo "Importing data"
    ./data_gen.py "$NUM_PARTITIONS" "$ROWS_PER_PARTITION" 2>&1 | tee "$RESULT_DIR"/import.out

    echo "Sleeping for $GRACE_PERIOD_IN_SECONDS_FOR_COMPACTIONS_TO_SETTLE seconds to let compactions settle"
    sleep "$GRACE_PERIOD_IN_SECONDS_FOR_COMPACTIONS_TO_SETTLE"

    echo "Recording sstable levels"
    ./"$CASSANDRA"/bin/nodetool cfstats ks 2>&1 > "$RESULT_DIR"/cfstats.out

    echo "Fetching data (warmup)"
    ./data_fetch.py "$NUM_FETCH_PROCESSES" "$NUM_PARTITIONS" "$NUM_FETCH_QUERIES" 2>&1 | tee "$RESULT_DIR"/warmup.out

    echo "Fetching data (real measurement)"
    ./data_fetch.py "$NUM_FETCH_PROCESSES" "$NUM_PARTITIONS" "$NUM_FETCH_QUERIES" 2>&1 | tee "$RESULT_DIR"/fetch.out

    echo "Finished benchmark (`date`)"
    kill "$CASSANDRA_PID"
    sleep "15"
}


if [ "$#" -eq 1 ] ; then
    run_benchmark "$1"
else
    echo "You must specify which cassandra to benchmark."
    exit 1
fi
