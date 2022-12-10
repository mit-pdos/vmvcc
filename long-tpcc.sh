#!/bin/bash

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=10

pushd benchmark
./sed-tplock.sh
popd

cc="tplock"

fpath=$dir/long-tpcc-$cc.csv
rm -f $fpath
# for nthrds in $(seq 16)
# do
# 	stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -duration $duration -debug false | tee -a $fpath
# done

for nthrds in $(seq 16)
do
	stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -stockscan -duration $duration -debug false | tee -a $fpath
done

pushd benchmark
./sed-mvcc.sh
popd

cc="mvcc"

fpath=$dir/long-tpcc-$cc.csv
rm -f $fpath
# for nthrds in $(seq 16)
# do
# 	stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -duration $duration -debug false | tee -a $fpath
# done

for nthrds in $(seq 16)
do
	stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -stockscan -duration $duration -debug false | tee -a $fpath
done
