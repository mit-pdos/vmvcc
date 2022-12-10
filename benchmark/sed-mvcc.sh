#!/bin/bash

sed -i 's/go-mvcc\/tplock/go-mvcc\/txn/' tpcc/*
sed -i 's/go-mvcc\/tplock/go-mvcc\/txn/' tpcc.go
sed -i 's/go-mvcc\/tplock/go-mvcc\/txn/' ycsb.go
sed -i '56s/NewROTxn()/New()/' tpcc.go
# sed -i '43s/NewROTxn()/New()/' ycsb.go
