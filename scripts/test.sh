#!/bin/bash

set -eu

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "$DIR/.."
go test ./vmvcc ./vmvcc ./txnsite ./index ./tuple ./wrbuf ./tid ./config ./common ./examples ./examples/strnum
