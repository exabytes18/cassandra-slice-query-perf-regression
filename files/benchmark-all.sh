#!/bin/bash

set -e -u

./benchmark.sh apache-cassandra-2.0.17
./benchmark.sh apache-cassandra-2.1.9
