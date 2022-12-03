package tpcc

import (
	// "fmt"
	"github.com/mit-pdos/go-mvcc/txn"
)

func stocklevel(
	txn *txn.Txn,
	/* input parameters */
	wid uint8, did uint8, threshold uint16,
	/* return value */
	cnt *uint32,
) bool {
	/* Read district. */
	district, _ := GetDistrict(txn, did, wid)
	oidub := district.D_NEXT_O_ID

	/* Computer the order id range. */
	var oidlb uint32
	if oidub < 20 {
		oidlb = 0
	} else {
		oidlb = oidub - 20
	}

	/**
	 * Use a map to count distinct items.
	 */
	iids := make(map[uint32]struct{}, 150)
	/* Read the latest 20 orders from OrderLine. */
	for oid := oidlb; oid < oidub; oid++ {
		/* Read all the items in this order. */
		for olnum := uint8(1); olnum <= 15; olnum++ {
			orderline, found := GetOrderLine(txn, oid, did, wid, olnum)
			if !found {
				break
			}
			iid := orderline.OL_I_ID
			stock, _ := GetStock(txn, iid, wid)
			quantity := stock.S_QUANTITY
			if quantity < threshold {
				iids[iid] = struct{}{}
			}
		}
	}

	/* Return the number of distinct items below the threshold. */
	*cnt = uint32(len(iids))

	return true
}

func TxnStockLevel(
	txno *txn.Txn,
	wid uint8, did uint8, threshold uint16,
) (uint32, bool) {
	var cnt uint32 = 0
	body := func(txni *txn.Txn) bool {
		return stocklevel(txni, wid, did, threshold, &cnt)
	}
	ok := txno.DoTxn(body)
	return cnt, ok
}
