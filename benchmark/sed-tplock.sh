#!/bin/bash

sed -i 's/go-mvcc\/txn/go-mvcc\/tplock/' tpcc/*
sed -i 's/go-mvcc\/txn/go-mvcc\/tplock/' tpcc.go
sed -i 's/go-mvcc\/txn/go-mvcc\/tplock/' ycsb.go
