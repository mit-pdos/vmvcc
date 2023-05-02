#!/bin/bash

read -n 1 -p 'This script uses git reset --hard, still run it (y/n)? ' answer
echo ''
if [ "$answer" != 'y' ]
then
	exit 1
fi

# Silo
./silo.sh 1

# Robustness to long-running readers
./long-ycsb.sh 1
./long-tpcc.sh 1

# Optimization factor analysis
git reset --hard && git apply base.diff     && ./optimization.sh 1 base
git reset --hard && git apply shardpad.diff && ./optimization.sh 1 shardpad
git reset --hard && git apply fai.diff      && ./optimization.sh 1 fai
git reset --hard && ./optimization.sh 1 rdtsc

# Scalability analysis
./scalability.sh 1
