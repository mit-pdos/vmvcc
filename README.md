# vMVCC: Verified transaction library using multi-version concurrency control

vMVCC is a transaction library aimed at reducing the effort of writing
**concurrent** application code.  It is implemented in Go and verified with
[Perennial](https://github.com/mit-pdos/perennial),
[Iris](https://iris-project.org/), and Coq.  This repository contains its
implementation.  You can find its formal specification and proof
[here](https://github.com/mit-pdos/perennial/tree/master/src/program_proof/mvcc).

For a high-level overview of its system architecture and proof, please refer to
our [OSDI'23 paper](https://pdos.csail.mit.edu/papers/vmvcc:osdi23.pdf).

## Limitations

1. Interface limited to `uint64` keys and `string` values
2. No durability
3. No range query

## Usage

See [`examples/hello.go`](examples/hello.go) for a minimal example of using
vMVCC.

### Import vMVCC

```go
import "github.com/mit-pdos/vmvcc/vmvcc"
```

### Creating a database and activating garbage collection

```go
func MkDB() *DB
func (db *DB) ActivateGC()
```

### Reading and writing the database

Define your **transaction body** with the following methods:

```go
func (txn *Txn) Read(key uint64) (string, bool)
func (txn *Txn) Write(key uint64, val string)
func (txn *Txn) Delete(key uint64) bool
```

### Executing transactions

Pick one of the approaches below to execute transactions.

#### Approach 1: `db.Run`

Pass your transaction body to `db.Run` to run the transaction atomically:

```go
func (db *DB) Run(body func(txn *Txn) bool) bool
```

It is safe to call `db.Run` concurrently on multiple threads.

#### Approach 2: `txn.Run`

To reduce memory allocation for transaction objects, and to have more control
over the assignment of transaction sites, another way to run transactions is
with the following approach: (1) create a transaction object `txn` with
`db.NewTxn`, and (2) call `txn.Run` to run the transaction atomically.

```go
func (db *DB) NewTxn() *Txn
func (txn *Txn) Run(body func(txn *Txn) bool) bool
```

You can reuse `txn` as many times as you want, and it is safe to call `Run`
concurrently with **different** transaction objects.  However, it is **NOT**
safe to call `Run` with the **same** transaction object concurrently.

#### Transactions with arguments

Both `Run` methods (on `Txn` and on `DB`) expect a function that takes a single
transaction object; use [function closures](https://go.dev/tour/moretypes/25) to
define your transaction with additional arguments. See
[`examples/xfer.go`](examples/xfer.go) for an example.

## Reproducing the results in the OSDI'23 paper

All the scripts for reproducing the results in the paper can be found in
[`osdi23/scripts`](osdi23/scripts).

Run all the experiments with `./osdi23/scripts/all.sh`.  The results will be
generated in the CSV format under the `./exp` directory.

## Developing

To build the code that should be working, run

```
./scripts/test.sh
```
