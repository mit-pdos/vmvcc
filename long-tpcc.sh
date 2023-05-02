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
nthrds=16

pushd benchmark
./sed-tplock.sh
popd

cc="tplock"

fpath=$dir/long-tpcc-$cc.csv
rm -f $fpath
for interval in 0 10000 5000 2000 1000 500 200 100
do
	for i in $(seq $niters)
	do
		stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -stockscan $interval -duration $duration -debug false | tee -a $fpath
	done
done

pushd benchmark
./sed-mvcc.sh
popd

cc="mvcc"

fpath=$dir/long-tpcc-$cc.csv
rm -f $fpath
for interval in 0 10000 5000 2000 1000 500 200 100
do
	for i in $(seq $niters)
	do
		stdbuf -o 0 go run ./benchmark/tpcc.go -nthrds $nthrds -stockscan $interval -duration $duration -debug false | tee -a $fpath
	done
done
