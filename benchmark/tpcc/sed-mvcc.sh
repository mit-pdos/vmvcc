#!/bin/bash

sed -i 's/go-mvcc\/tplock/go-mvcc\/txn/' *
sed -i 's/go-mvcc\/tplock/go-mvcc\/txn/' ../tpcc.go
