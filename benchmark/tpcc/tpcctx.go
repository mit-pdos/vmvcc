/**
 * TPC-C context to reduce memory presure induced by the benchmark itself.
 */
package main

type TPCContext struct {
	warehouse Warehouse
	district  District
	customer  Customer
	history   History
	neworder  NewOrder
	orderline OrderLine
	items     []Item
	stock     Stock
	noparam   NewOrderInput
	pparam    PaymentInput
	osparam   OrderStatusInput
	dparam    DeliveryInput
	slparam   StockLevelInput
}

func NewTPCContext() *TPCContext {
	ctx := new(TPCContext)
	ctx.items = make([]Item, OL_MAX_CNT)
	return ctx
}
