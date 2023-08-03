#!/bin/bash

if [ -z "$1" ]
then
	nruns=1
else
	nruns=$1
fi

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=3
rkeys=1000000

theta=0.85
nkeys=4
nthrds=24

./osdi23/scripts/sed-tplock.sh

cc="tplock"

fpath=$dir/long-ycsb-$cc.csv
rm -f $fpath
for i in $(seq $nruns)
do
	for rdratio in 100 80 60 40 20 0
	do
		stdbuf -o 0 go run ./benchmark/ycsb -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
	done
	for rdratio in 100 80 60 40 20 0
	do
		stdbuf -o 0 go run ./benchmark/ycsb -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -long -exp | tee -a $fpath
	done
done

./osdi23/scripts/sed-mvcc.sh

cc="mvcc"

fpath=$dir/long-ycsb-$cc.csv
rm -f $fpath
for i in $(seq $nruns)
do
	for rdratio in 100 80 60 40 20 0
	do
		stdbuf -o 0 go run ./benchmark/ycsb -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
	done
	for rdratio in 100 80 60 40 20 0
	do
		stdbuf -o 0 go run ./benchmark/ycsb -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -long -exp | tee -a $fpath
	done
done

