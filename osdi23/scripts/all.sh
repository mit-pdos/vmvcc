#!/bin/bash

read -n 1 -p 'This script uses git reset --hard, still run it (y/n)? ' answer
echo ''
if [ "$answer" != 'y' ]
then
	exit 1
fi

nruns=1

dir=./osdi23/scripts

# Silo
$dir/silo.sh $nruns

# Robustness to long-running readers
$dir/long-ycsb.sh $nruns
$dir/long-tpcc.sh $nruns

# Optimization factor analysis
git reset --hard && git apply $dir/base.diff     && $dir/optimization.sh $nruns base
git reset --hard && git apply $dir/shardpad.diff && $dir/optimization.sh $nruns shardpad
git reset --hard && git apply $dir/fai.diff      && $dir/optimization.sh $nruns fai
git reset --hard && $dir/optimization.sh $nruns rdtsc

# Scalability analysis
$dir/scalability.sh $nruns
