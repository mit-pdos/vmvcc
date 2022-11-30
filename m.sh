#!/bin/bash

dir=./exp
rm -rf $dir
mkdir $dir

duration=30

fpath=$dir/micro-read.csv
for rkeys in 100000
do
	for nkeys in 4
	do
		for nthrds in $(seq 9)
		do
			#echo "rkeys = $rkeys; nkeys = $nkeys; nthrds = $nthrds"
			stdbuf -o 0 perflock numactl --physcpubind=+0-$nthrds go run ./benchmark/micro-read/ -rkeys $rkeys -nkeys $nkeys -nthrds $nthrds -duration $duration -exp | tee -a $fpath
		done
	done
done

fpath=$dir/micro-write.csv
for rkeys in 100000
do
	for nkeys in 4
	do
		for nthrds in $(seq 9)
		do
			#echo "rkeys = $rkeys; nkeys = $nkeys; nthrds = $nthrds"
			stdbuf -o 0 perflock numactl --physcpubind=+0-$nthrds go run ./benchmark/micro-write/ -rkeys $rkeys -nkeys $nkeys -nthrds $nthrds -duration $duration -exp | tee -a $fpath
		done
	done
done

fpath=$dir/micro-hybrid.csv
for rkeys in 100000
do
	for nkeys in 4
	do
		for rdratio in 20 40 60 80
		do
			for nthrds in $(seq 1)
			do
				#echo "rkeys = $rkeys; nkeys = $nkeys; nthrds = $nthrds"
				stdbuf -o 0 perflock numactl --physcpubind=+0-$nthrds go run ./benchmark/micro-hybrid/ -rkeys $rkeys -nkeys $nkeys -rdratio $rdratio -nthrds $nthrds -duration $duration -exp | tee -a $fpath
			done
		done
	done
done

fpath=$dir/micro-long.csv
for rkeys in 100000
do
	for nkeys in 4
	do
		for nthrds in $(seq 8)
		do
			#echo "rkeys = $rkeys; nkeys = $nkeys; nthrds = $nthrds"
			stdbuf -o 0 perflock numactl --physcpubind=+0-9 go run ./benchmark/micro-long/ -rkeys $rkeys -nkeys $nkeys -nthrds $nthrds -duration $duration -exp | tee -a $fpath
		done
	done
done
