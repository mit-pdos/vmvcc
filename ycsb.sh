#!/bin/bash

dir=./exp
# rm -rf $dir
mkdir -p $dir

cc=mvcc
duration=60
rkeys=10000000

fpath=$dir/ycsb-$cc.csv
for rdratio in 0 10 20 30 40 50 60 70 80 90 100
do
	# for nthrds in $(seq 8)
	for nthrds in 8
	do
		stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -rkeys $rkeys -exp | tee -a $fpath
	done
done

fpath=$dir/ycsb-long-$cc.csv
for rdratio in 0 10 20 30 40 50 60 70 80 90 100
do
	# for nthrds in $(seq 8)
	for nthrds in 8
	do
		stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -rkeys $rkeys -long -exp | tee -a $fpath
	done
done
