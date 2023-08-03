#!/bin/bash

sed -i 's/vmvcc\/vmvcc/vmvcc\/osdi23\/tplock/' ./benchmark/tpcc/*.go
sed -i 's/vmvcc\/vmvcc/vmvcc\/osdi23\/tplock/' ./benchmark/ycsb/ycsb.go
sed -i '55s/NewTxn()/NewROTxn()/' ./benchmark/tpcc/tpcc.go
