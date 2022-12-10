#!/bin/bash

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=30
rkeys=1000000
theta=0.1

nkeys=1

fpath=$dir/silo-ycsb.csv
rm -f $fpath
for rdratio in 0 50 100
do
	# for nthrds in $(seq 16)
	for nthrds in 16
	do
		stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
	done
done

duration=30

fpath=$dir/silo-tpcc.csv
rm -f $fpath
for workloads in '45,43,4,4,4'
do
	# for nthrds in $(seq 8)
	for nthrds in 1 2 4 8 16
	do
		stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -duration $duration -workloads $workloads -debug false | tee -a $fpath
	done
done
