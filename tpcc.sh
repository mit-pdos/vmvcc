#!/bin/bash

dir=./exp
rm -rf $dir
mkdir $dir

duration=30

fpath=$dir/tpcc.csv
for workloads in '100,0,0,0,0' '0,100,0,0,0' '0,0,100,0,0' '0,0,0,100,0' '0,0,0,0,100' '45,43,4,4,4'
do
	for nthrds in $(seq 8)
	do
		#echo "rkeys = $rkeys; nkeys = $nkeys; nthrds = $nthrds"
		stdbuf -o 0 numactl --physcpubind=+0-9 go run ./benchmark/tpcc.go -nthrds $nthrds -duration $duration -workloads $workloads | tee -a $fpath
	done
done
