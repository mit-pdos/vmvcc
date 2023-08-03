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
nthrds=32

./osdi23/scripts/sed-tplock.sh

cc="tplock"

fpath=$dir/long-tpcc-$cc.csv
rm -f $fpath
for i in $(seq $nruns)
do
	for interval in 0 10000 5000 1000 500 100
	do
		stdbuf -o 0 go run ./benchmark/tpcc -nthrds $nthrds -stockscan $interval -duration $duration -debug false | tee -a $fpath
	done
done

./osdi23/scripts/sed-mvcc.sh

cc="mvcc"

fpath=$dir/long-tpcc-$cc.csv
rm -f $fpath
for i in $(seq $nruns)
do
	for interval in 0 10000 5000 1000 500 100
	do
		stdbuf -o 0 go run ./benchmark/tpcc -nthrds $nthrds -stockscan $interval -duration $duration -debug false | tee -a $fpath
	done
done
