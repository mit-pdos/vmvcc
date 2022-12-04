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

func (g *Generator) GetPaymentInput() *PaymentInput {
	p := &PaymentInput {
		W_ID : g.wid,
		D_ID : uint8(g.rd.Uint32() % uint32(g.nLocalDistricts)) + 1,
		H_AMOUNT : 2.5,
		C_W_ID : uint8(g.rd.Uint32() % uint32(g.nWarehouses)) + 1,
		C_D_ID : uint8(g.rd.Uint32() % uint32(g.nLocalDistricts)) + 1,
		C_ID : g.rd.Uint32() % uint32(g.nLocalCustomers) + 1,
	}
	return p
}

func (g *Generator) GetNewOrderInput() *NewOrderInput {
	p := &NewOrderInput {
		W_ID : g.wid,
	}
	return p
}
