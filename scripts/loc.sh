#!/bin/bash

DIRS="txn wrbuf tuple index tid cfmutex config"
FILES_EXEC=$(find ${DIRS} | grep -E ".go$" | grep -vE "_test.go$" | tr '\n' ' ')
FILES_TEST=$(find ${DIRS} | grep -E "_test.go$" | tr '\n' ' ')

echo "Executable files:"
wc -l $FILES_EXEC

echo "Testing files:"
wc -l $FILES_TEST

