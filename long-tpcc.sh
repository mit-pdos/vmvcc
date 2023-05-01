#!/bin/bash

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=30
nthrds=16

pushd benchmark
./sed-tplock.sh
popd

cc="tplock"

fpath=$dir/long-tpcc-$cc.csv
rm -f $fpath
for interval in 0 10000 5000 2000 1000 500 200 100
do
	stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -stockscan $interval -duration $duration -debug false | tee -a $fpath
done

pushd benchmark
./sed-mvcc.sh
popd

cc="mvcc"

fpath=$dir/long-tpcc-$cc.csv
rm -f $fpath
for interval in 0 10000 5000 2000 1000 500 200 100
do
	stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -stockscan $interval -duration $duration -debug false | tee -a $fpath
done
