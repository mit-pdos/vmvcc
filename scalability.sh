#!/bin/bash

if [ -z "$1" ]
then
	niters=1
else
	niters=$1
fi

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=30
rkeys=1000000

nkeys=4

for rdratio in 0 100
do
	for theta in 0.8 0.85 0.9 0.95
	do
		fpath=$dir/scalability-$rdratio-$theta.csv
		rm -f $fpath
		for nthrds in 1 2 4 8 16 32
		# for nthrds in 4
		do
			for i in $(seq $niters)
			do
				stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
			done
		done
	done
done
