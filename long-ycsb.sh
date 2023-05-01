#!/bin/bash

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=30
rkeys=1000000

theta=0.85
nkeys=4
nthrds=8

pushd benchmark
./sed-tplock.sh
popd

cc="tplock"

fpath=$dir/long-ycsb-$cc.csv
rm -f $fpath
for rdratio in 100 80 60 40 20 0
do
	stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
done
for rdratio in 100 80 60 40 20 0
do
	stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -long -exp | tee -a $fpath
done

pushd benchmark
./sed-mvcc.sh
popd

cc="mvcc"

fpath=$dir/long-ycsb-$cc.csv
rm -f $fpath
for rdratio in 100 80 60 40 20 0
do
	stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
done

fpath=$dir/long-ycsb-$cc.csv
for rdratio in 100 80 60 40 20 0
do
	stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -long -exp | tee -a $fpath
done

