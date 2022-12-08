#!/bin/bash

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=10
rkeys=100000

cc="mvcc"
theta=0.9
nkeys=4

fpath=$dir/ycsb-$cc-$theta.csv
for rdratio in 0 20 40 60 80 100
do
	# for nthrds in $(seq 8)
	for nthrds in 4
	do
		stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
	done
done

fpath=$dir/ycsb-long-$cc-$theta.csv
for rdratio in 0 20 40 60 80 100
do
	# for nthrds in $(seq 8)
	for nthrds in 4
	do
		stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -long -exp | tee -a $fpath
	done
done
