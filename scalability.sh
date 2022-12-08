#!/bin/bash

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=10
rkeys=100000

nkeys=4

for rdratio in 0 100
do
	for theta in 0.8 0.85 0.9 0.95
	do
		fpath=$dir/ycsb-$rdratio-$theta.csv
		for nthrds in $(seq 8)
		# for nthrds in 4
		do
			stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
		done
	done
done
