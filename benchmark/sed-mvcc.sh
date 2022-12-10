#!/bin/bash

sed -i 's/go-mvcc\/tplock/go-mvcc\/txn/' tpcc/*
sed -i 's/go-mvcc\/tplock/go-mvcc\/txn/' tpcc.go
sed -i 's/go-mvcc\/tplock/go-mvcc\/txn/' ycsb.go
