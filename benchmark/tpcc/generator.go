/**
 * Based on:
 * https://github.com/apavlo/py-tpcc/blob/7c3ff501bbe98a6a7abd3c13267523c3684b62d6/pytpcc/runtime/executor.py
 */

package tpcc

import (
	"math/rand"
)

const (
	TXN_NEWORDER = iota
	TXN_PAYMENT
	TXN_ORDERSTATUS
	TXN_DELIVERY
	TXN_STOCKLEVEL
)

type Generator struct {
	rd              *rand.Rand
	wvec            []uint64
	wid             uint8
	nItems          uint32
	nWarehouses     uint8
	nLocalDistricts uint8
	nLocalCustomers uint32
}

func NewGenerator(
	wid uint8,
	workloads []uint64,
	nItems uint32,
	nWarehouses uint8,
	nLocalDistricts uint8,
	nLocalCustomers uint32,
) *Generator {
	wvec := make([]uint64, 5)
	var accu uint64 = 0
	for i, x := range workloads {
		wvec[i] = accu + x
		accu += x
	}
	rd := rand.New(rand.NewSource(int64(wid)))
	gen := &Generator {
		rd  : rd,
		wvec : wvec,
		wid : wid,
		nItems : nItems,
		nWarehouses : nWarehouses,
		nLocalDistricts : nLocalDistricts,
		nLocalCustomers : nLocalCustomers,
	}

	return gen
}

func (g *Generator) PickTxn() int {
	x := g.rd.Uint64() % 100
	if x < g.wvec[0] {
		return TXN_NEWORDER
	} else if x < g.wvec[1] {
		return TXN_PAYMENT
	} else if x < g.wvec[2] {
		return TXN_ORDERSTATUS
	} else if x < g.wvec[3] {
		return TXN_DELIVERY
	} else {
		return TXN_STOCKLEVEL
	}
}

func (g *Generator) GetNewOrderInput() *NewOrderInput {
	/* Generate item info (including iid, wid, and qty) in the new order. */
	n := pickNOrderLines(g.rd)
	iids := make([]uint32, n)
	iwids := make([]uint8, n)
	iqtys := make([]uint8, n)
	for i := range iids {
		iids[i] = g.iid()
		iqtys[i] = pickQuantity(g.rd)
		/* 1% of order lines are remote. */
		if trueWithProb(g.rd, 1) {
			iwids[i] = pickWarehouseIdExcept(g.rd, g.nWarehouses, g.wid)
		} else {
			iwids[i] = g.wid
		}
	}

	p := &NewOrderInput {
		W_ID : g.wid,
		D_ID : g.did(),
		C_ID : g.cid(),
		O_ENTRY_D : getTime(),
		I_IDS : iids,
		I_W_IDS : iwids,
		I_QTYS : iqtys,
	}
	return p
}

func (g *Generator) GetPaymentInput() *PaymentInput {
	/* 15% of payments are remote, i.e., W_ID != C_W_ID. */
	did := g.did()
	var cwid, cdid uint8
	if trueWithProb(g.rd, 15) {
		cwid = pickWarehouseIdExcept(g.rd, g.nWarehouses, g.wid)
		cdid = g.did()
	} else {
		cwid = g.wid
		cdid = did
	}

	p := &PaymentInput {
		W_ID : g.wid,
		D_ID : did,
		H_AMOUNT : 2.5 /* TODO */,
		C_W_ID : cwid,
		C_D_ID : cdid,
		C_ID : g.cid(),
		H_DATE : getTime(),
	}
	return p
}

func (g *Generator) GetOrderStatusInput() *OrderStatusInput {
	p := &OrderStatusInput {
		W_ID : g.wid,
		D_ID : g.did(),
		C_ID : g.cid(),
	}
	return p
}

func (g *Generator) GetDeliveryInput() *DeliveryInput {
	p := &DeliveryInput {
		W_ID : g.wid,
		O_CARRIER_ID : uint8(pickBetween(g.rd, 1, 10)),
		OL_DELIVERY_D : getTime(),
	}
	return p
}

func (g *Generator) GetStockLevelInput() *StockLevelInput {
	p := &StockLevelInput {
		W_ID : g.wid,
		D_ID : g.did(),
		THRESHOLD : uint16(pickBetween(g.rd, 10, 20)),
	}
	return p
}

func (g *Generator) did() uint8 {
	n := uint8(pickBetween(g.rd, 1, uint32(g.nLocalDistricts)))
	return n
}

func (g *Generator) cid() uint32 {
	/* See Silo tpcc.cc:L376. */
	n := uint32(pickBetweenNonUniformly(g.rd, 1023, 259, 1, g.nLocalCustomers))
	return n
}

func (g *Generator) iid() uint32 {
	/* See Silo tpcc.cc:L369. */
	n := uint32(pickBetweenNonUniformly(g.rd, 8191, 7911, 1, g.nItems))
	return n
}
