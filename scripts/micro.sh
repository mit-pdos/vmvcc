#!/bin/bash

dir=./exp
rm -rf $dir
mkdir $dir

fpath=$dir/micro-read.csv
for rkeys in 1000 100000
do
	for nkeys in 1 10 100
	do
		for nthrds in $(seq 60)
		do
			echo "rkeys = $rkeys; nkeys = $nkeys; nthrds = $nthrds"
			#stdbuf -o 0 perflock numactl --cpunodebind=0 go run ./benchmark/micro-read/ -rkeys $rkeys -nkeys $nkeys -nthrds $nthrds -exp | tee -a $fpath
			stdbuf -o 0 perflock numactl --physcpubind=+0-$nthrds go run ./benchmark/micro-read/ -rkeys $rkeys -nkeys $nkeys -nthrds $nthrds -exp | tee -a $fpath
		done
	done
done

fpath=$dir/micro-write.csv
for rkeys in 1000 100000
do
	for nkeys in 1 10 100
	do
		for nthrds in $(seq 60)
		do
			echo "rkeys = $rkeys; nkeys = $nkeys; nthrds = $nthrds"
			#stdbuf -o 0 perflock numactl --cpunodebind=0 go run ./benchmark/micro-write/ -rkeys $rkeys -nkeys $nkeys -nthrds $nthrds -exp | tee -a $fpath
			stdbuf -o 0 perflock numactl --physcpubind=+0-$nthrds go run ./benchmark/micro-write/ -rkeys $rkeys -nkeys $nkeys -nthrds $nthrds -exp | tee -a $fpath
		done
	done
done
