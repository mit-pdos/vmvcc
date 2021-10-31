package txn

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"go-mvcc/index"
)

func TestNew(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()
	txn := txnMgr.New(idx)
	assert.Equal(len(txn.wset), 0)
}
