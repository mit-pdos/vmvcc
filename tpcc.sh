#!/bin/bash

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=30

fpath=$dir/tpcc-mvcc-std-no50sl50.csv
rm -f $fpath
for workloads in '45,43,4,4,4' '50,0,0,0,50'
do
	for nthrds in $(seq 8)
	# for nthrds in 1 2 4 8
	do
		stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -duration $duration -workloads $workloads -debug false | tee -a $fpath
	done
done
