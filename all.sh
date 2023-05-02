#!/bin/bash

read -n 1 -p 'This script uses git reset --hard, still run it (y/n)? ' answer
echo ''
if [ "$answer" != 'y' ]
then
	exit 1
fi

nruns=3

# Silo
./silo.sh $nruns

# Robustness to long-running readers
./long-ycsb.sh $nruns
./long-tpcc.sh $nruns

# Optimization factor analysis
git reset --hard && git apply base.diff     && ./optimization.sh $nruns base
git reset --hard && git apply shardpad.diff && ./optimization.sh $nruns shardpad
git reset --hard && git apply fai.diff      && ./optimization.sh $nruns fai
git reset --hard && ./optimization.sh $nruns rdtsc

# Scalability analysis
./scalability.sh $nruns
