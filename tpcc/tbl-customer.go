package tpcc

import (
	"github.com/tchajed/marshal"
)

func readCustomer(txn *Txn, tplid uint64) Customer {
	var k = make([]byte, 0, 8)
	k = marshal.WriteInt(k, tplid)

	v, _ := txn.Read(MAPID_CUSTOMER_TBL, k)
	/**
	 * We don't even check if this customer exists, as this is always called
	 * after a `findCustomerByCID`, and by index referential integrity we can
	 * deduce its existence.
	 */

	cid, v1 := marshal.ReadInt(v)
	cwid, clast := marshal.ReadInt(v1)
	return Customer {
		C_ID : cid,
		C_W_ID : cwid,
		C_LAST : clast,
	}
}

func findCustomerByCID(txn *Txn, cid uint64, cwid uint64) (uint64, bool) {
	var k = make([]byte, 0, 16)
	k = marshal.WriteInt(k, cid)
	k = marshal.WriteInt(k, cwid)

	v, ok := txn.Read(MAPID_CUSTOMER_IDX_CID, k)
	if !ok {
		return 0, false
	}

	tplid, _ := marshal.ReadInt(v)
	return tplid, true
}

func findCustomersByLast(txn *Txn, clast []byte, cwid uint64) []uint64 {
	var k = make([]byte, 0, 16)
	k = marshal.WriteBytes(k, clast)
	k = marshal.WriteInt(k, cwid)

	_, ok := txn.Read(MAPID_CUSTOMER_IDX_LAST, k)
	var tplids = make([]uint64, 0)
	if !ok {
		return tplids
	}

	/* TODO: decode []uint64. */
	return tplids
}

func SelectCustomerByCID(txn *Txn, cid uint64, cwid uint64, cref *Customer) bool {
	tplid, found := findCustomerByCID(txn, cid, cwid)
	if !found {
		return false
	}
	customer := readCustomer(txn, tplid)
	/* FIXME: Perennial annot reduce this. */
	// *cref = customer
	cref.C_ID = customer.C_ID
	cref.C_W_ID = customer.C_W_ID
	cref.C_LAST = customer.C_LAST
	return true
}

func SelectCustomersByLast(txn *Txn, clast []byte, cwid uint64, csref *[]Customer) bool {
	tplids := findCustomersByLast(txn, clast, cwid)
	var customers = make([]Customer, 0, uint64(len(tplids)))
	for _, tplid := range(tplids) {
		/* We don't need to check `ok` with index integrity. */
		c := readCustomer(txn, tplid)
		customers = append(customers, c)
	}
	*csref = customers
	return true
}

/**
 * Note that functions below are not the interface of this file.
 */
func TxnSelectCustomerByCID(t *Txn, cid uint64, cwid uint64) Customer {
	var c Customer
	body := func(txn *Txn) bool {
		return SelectCustomerByCID(txn, cid, cwid, &c)
	}
	t.DoTxn(body)
	/**
	 * Read-only transactions always succeed, so we don't bother checking the
	 * result of `DoTxn`.
	 */
	return c
}

/* TODO: TxnSelectCustomerByLast */
