#!/bin/bash

if [ -z "$1" ]
then
	niters=1
else
	niters=$1
fi

GO=go
dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=30
rkeys=1000000
theta=-1

nkeys=1

for i in $(seq 1)
do
	fpath=$dir/silo-ycsb.csv
	rm -f $fpath
	for rdratio in 100 50 0 200
	do
		# for nthrds in $(seq 16)
		for nthrds in 16
		do
			for i in $(seq $niters)
			do
				stdbuf -o 0 $GO run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
			done
		done
	done

	fpath=$dir/silo-tpcc.csv
	rm -f $fpath
	for workloads in '45,43,4,4,4'
	do
		# for nthrds in $(seq 8)
		for nthrds in 1 8
		do
			for i in $(seq $niters)
			do
				stdbuf -o 0 $GO run ./benchmark/tpcc.go -nthrds $nthrds -duration $duration -workloads $workloads -debug false | tee -a $fpath
			done
		done
	done
done
