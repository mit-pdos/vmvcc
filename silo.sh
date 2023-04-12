#!/bin/bash

GO=go1.20.2
dir=./exp/silo-${GO}
rm -rf $dir
mkdir -p $dir

duration=30
rkeys=1000000
theta=-1

nkeys=1

for i in $(seq 10)
do
	fpath=$dir/ycsb.csv
	# rm -f $fpath
	for rdratio in 100 50 0 200
	do
		# for nthrds in $(seq 16)
		for nthrds in 16
		do
			stdbuf -o 0 $GO run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
		done
	done

	# fpath=$dir/tpcc.csv
	# # rm -f $fpath
	# for workloads in '45,43,4,4,4'
	# do
	# 	# for nthrds in $(seq 8)
	# 	for nthrds in 1 8
	# 	do
	# 		stdbuf -o 0 $GO run ./benchmark/tpcc.go -nthrds $nthrds -duration $duration -workloads $workloads -debug false | tee -a $fpath
	# 	done
	# done
done
