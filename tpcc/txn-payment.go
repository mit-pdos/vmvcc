package tpcc

func payment(txn *Txn) bool {
	return true
}

func TxnPayment(t *Txn) bool {
	body := func(txn *Txn) bool {
		return payment(txn)
	}
	ok := t.DoTxn(body)
	return ok
}
