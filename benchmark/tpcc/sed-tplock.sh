#!/bin/bash

sed -i 's/go-mvcc\/txn/go-mvcc\/tplock/' *
sed -i 's/go-mvcc\/txn/go-mvcc\/tplock/' ../tpcc.go
