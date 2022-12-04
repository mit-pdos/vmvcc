/**
 * Based on:
 * https://github.com/apavlo/py-tpcc/blob/7c3ff501bbe98a6a7abd3c13267523c3684b62d6/pytpcc/runtime/executor.py
 */

package tpcc

import (
	"math/rand"
)

type Generator struct {
	rd  *rand.Rand
	wid uint8
	nItems uint32
	nWarehouses uint8
	nLocalDistricts uint8
	nLocalCustomers uint32
}

func NewGenerator(
	src rand.Source,
	wid uint8,
	nItems uint32,
	nWarehouses uint8,
	nLocalDistricts uint8,
	nLocalCustomers uint32,
) *Generator {
	gen := &Generator {
		rd  : rand.New(src),
		wid : wid,
		nItems : nItems,
		nWarehouses : nWarehouses,
		nLocalDistricts : nLocalDistricts,
		nLocalCustomers : nLocalCustomers,
	}

	return gen
}

func (g *Generator) GetNewOrderInput() *NewOrderInput {
	p := &NewOrderInput {
		W_ID : g.wid,
	}
	return p
}

func (g *Generator) GetPaymentInput() *PaymentInput {
	/* 15% of payments are remote, i.e., W_ID != C_W_ID. */
	did := g.did()
	var cwid, cdid uint8
	if g.rd.Uint32() % 100 < 15 {
		cwid = pickWarehouseIdExcept(g.nWarehouses, g.wid)
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
		H_DATE : 0 /* TODO */,
	}
	return p
}

func (g *Generator) GetOrderStatusInput() *OrderStatusInput {
	p := &OrderStatusInput {
		W_ID : g.wid,
	}
	return p
}

func (g *Generator) GetDeliveryInput() *DeliveryInput {
	p := &DeliveryInput {
		W_ID : g.wid,
	}
	return p
}

func (g *Generator) GetStockLevelInput() *StockLevelInput {
	p := &StockLevelInput {
		W_ID : g.wid,
	}
	return p
}

func (g *Generator) did() uint8 {
	return uint8(g.rd.Uint32() % uint32(g.nLocalDistricts)) + 1
}

func (g *Generator) cid() uint32 {
	return g.rd.Uint32() % uint32(g.nLocalCustomers) + 1
}

func (g *Generator) iid() uint32 {
	return g.rd.Uint32() % uint32(g.nItems) + 1
}
