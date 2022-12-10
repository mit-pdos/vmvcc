#!/bin/bash

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=60
rkeys=10000000

theta=0.2
nkeys=1
rdratio=100

fpath=$dir/ycsb-optimization-$1.csv
rm -f $fpath
for nthrds in $(seq 16)
do
	stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
done
