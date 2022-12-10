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
for interval in 100 200 400 800 1600 3200 6400
do
	stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -stockscan $interval -duration $duration -debug false | tee -a $fpath
done

pushd benchmark
./sed-mvcc.sh
popd

cc="mvcc"

fpath=$dir/long-tpcc-$cc.csv
rm -f $fpath
for interval in 100 200 400 800 1600 3200 6400
do
	stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -stockscan $interval -duration $duration -debug false | tee -a $fpath
done
