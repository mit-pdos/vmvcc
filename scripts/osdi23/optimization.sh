#!/bin/bash

if [ -z "$1" ]
then
	nruns=1
else
	nruns=$1
fi

if [ -z "$2" ]
then
	echo 'Please specify configuration name.'
	exit 1
fi

dir=./exp
# rm -rf $dir
mkdir -p $dir

duration=30
rkeys=1000000

theta=0.2
nkeys=1
rdratio=100

fpath=$dir/optimization-$2.csv
rm -f $fpath
for i in $(seq $nruns)
do
	for nthrds in 1 2 4 8 16 32
	do
		stdbuf -o 0 go run ./benchmark/ycsb.go -nthrds $nthrds -duration $duration -rdratio $rdratio -nkeys $nkeys -rkeys $rkeys -theta $theta -exp | tee -a $fpath
	done
done
