#!/bin/bash

sed -i 's/vmvcc\/osdi23\/tplock/vmvcc\/vmvcc/' ./benchmark/tpcc/*.go
sed -i 's/vmvcc\/osdi23\/tplock/vmvcc\/vmvcc/' ./benchmark/ycsb/ycsb.go
sed -i '55s/NewROTxn()/NewTxn()/' ./benchmark/tpcc/tpcc.go
