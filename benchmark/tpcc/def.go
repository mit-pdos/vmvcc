package main

/**
 * Table definitions.
 */
type Warehouse struct {
	/* Primary key: W_ID */
	W_ID uint8
	/* Data fields */
	W_NAME     [10]byte
	W_STREET_1 [20]byte
	W_STREET_2 [20]byte
	W_CITY     [20]byte
	W_STATE    [2]byte
	W_ZIP      [9]byte
	W_TAX      float32
	W_YTD      float32
}

const X_W_ID uint64 = 0
const X_W_NAME uint64 = 1
const X_W_STREET_1 uint64 = 11
const X_W_STREET_2 uint64 = 31
const X_W_CITY uint64 = 51
const X_W_STATE uint64 = 71
const X_W_ZIP uint64 = 73
const X_W_TAX uint64 = 82
const X_W_YTD uint64 = 86
const X_W_LEN uint64 = 90

type District struct {
	/* Primary key: (D_W_ID, D_ID) */
	D_ID   uint8
	D_W_ID uint8
	/* Data fields */
	D_NAME      [10]byte
	D_STREET_1  [20]byte
	D_STREET_2  [20]byte
	D_CITY      [20]byte
	D_STATE     [2]byte
	D_ZIP       [9]byte
	D_TAX       float32
	D_YTD       float32
	D_NEXT_O_ID uint32
	/* TPC-C does not have this field, but this makes DELIVERY more efficient */
	D_OLD_O_ID uint32
}

const X_D_ID uint64 = 0
const X_D_W_ID uint64 = 1
const X_D_NAME uint64 = 2
const X_D_STREET_1 uint64 = 12
const X_D_STREET_2 uint64 = 32
const X_D_CITY uint64 = 52
const X_D_STATE uint64 = 72
const X_D_ZIP uint64 = 74
const X_D_TAX uint64 = 83
const X_D_YTD uint64 = 87
const X_D_NEXT_O_ID uint64 = 91
const X_D_OLD_O_ID uint64 = 95
const X_D_LEN uint64 = 99

type Customer struct {
	/* Primary key: (C_ID, C_D_ID, C_W_ID) */
	C_ID   uint32
	C_D_ID uint8
	C_W_ID uint8
	/* Data fields */
	C_FIRST        [16]byte
	C_MIDDLE       [2]byte
	C_LAST         [16]byte
	C_STREET_1     [20]byte
	C_STREET_2     [20]byte
	C_CITY         [20]byte
	C_STATE        [2]byte
	C_ZIP          [9]byte
	C_PHONE        [16]byte
	C_SINCE        uint32
	C_CREDIT       [2]byte
	C_CREDIT_LIM   float32
	C_DISCOUNT     float32
	C_BALANCE      float32
	C_YTD_PAYMENT  float32
	C_PAYMENT_CNT  uint16
	C_DELIVERY_CNT uint16
	C_DATA         [500]byte
}

const X_C_ID uint64 = 0
const X_C_D_ID uint64 = 4
const X_C_W_ID uint64 = 5
const X_C_FIRST uint64 = 6
const X_C_MIDDLE uint64 = 22
const X_C_LAST uint64 = 24
const X_C_STREET_1 uint64 = 40
const X_C_STREET_2 uint64 = 60
const X_C_CITY uint64 = 80
const X_C_STATE uint64 = 100
const X_C_ZIP uint64 = 102
const X_C_PHONE uint64 = 111
const X_C_SINCE uint64 = 127
const X_C_CREDIT uint64 = 131
const X_C_CREDIT_LIM uint64 = 133
const X_C_DISCOUNT uint64 = 137
const X_C_BALANCE uint64 = 141
const X_C_YTD_PAYMENT uint64 = 145
const X_C_PAYMENT_CNT uint64 = 149
const X_C_DELIVERY_CNT uint64 = 151
const X_C_DATA uint64 = 153
const X_C_LEN uint64 = 653

type History struct {
	/* Primary key: H_ID (no primary key required in the spec) */
	H_ID uint64 /* the MSB, reserved for table ID, should not be used */
	/* Data fields */
	H_C_ID   uint32
	H_C_D_ID uint8
	H_C_W_ID uint8
	H_D_ID   uint8
	H_W_ID   uint8
	H_DATE   uint32
	H_AMOUNT float32
	H_DATA   [25]byte
}

const X_H_ID uint64 = 0
const X_H_C_ID uint64 = 8
const X_H_C_D_ID uint64 = 12
const X_H_C_W_ID uint64 = 13
const X_H_D_ID uint64 = 14
const X_H_W_ID uint64 = 15
const X_H_DATE uint64 = 16
const X_H_AMOUNT uint64 = 20
const X_H_DATA uint64 = 24
const X_H_LEN uint64 = 49

type NewOrder struct {
	/* Primary key: (NO_O_ID, NO_D_ID, NO_W_ID) */
	NO_O_ID uint32
	NO_D_ID uint8
	NO_W_ID uint8
	/* No data fields */
}

const X_NO_O_ID uint64 = 0
const X_NO_D_ID uint64 = 4
const X_NO_W_ID uint64 = 5
const X_NO_LEN uint64 = 6

type Order struct {
	/* Primary key: (O_W_ID, O_D_ID, O_ID) */
	O_ID   uint32
	O_D_ID uint8
	O_W_ID uint8
	/* Data fields */
	O_C_ID       uint32
	O_ENTRY_D    uint32
	O_CARRIER_ID uint8
	O_OL_CNT     uint8
	O_ALL_LOCAL  bool
}

const X_O_ID uint64 = 0
const X_O_D_ID uint64 = 4
const X_O_W_ID uint64 = 5
const X_O_C_ID uint64 = 6
const X_O_ENTRY_D uint64 = 10
const X_O_CARRIER_ID uint64 = 14
const X_O_OL_CNT uint64 = 15
const X_O_ALL_LOCAL uint64 = 16
const X_O_LEN uint64 = 17

type OrderLine struct {
	/* Primary key: (OL_W_ID, OL_D_ID, OL_W_ID, OL_NUMBER) */
	OL_O_ID   uint32
	OL_D_ID   uint8
	OL_W_ID   uint8
	OL_NUMBER uint8
	/* Data fields */
	OL_I_ID        uint32
	OL_SUPPLY_W_ID uint8
	OL_DELIVERY_D  uint32
	OL_QUANTITY    uint8
	OL_AMOUNT      float32
}

const X_OL_O_ID uint64 = 0
const X_OL_D_ID uint64 = 4
const X_OL_W_ID uint64 = 5
const X_OL_NUMBER uint64 = 6
const X_OL_I_ID uint64 = 7
const X_OL_SUPPLY_W_ID uint64 = 11
const X_OL_DELIVERY_D uint64 = 12
const X_OL_QUANTITY uint64 = 16
const X_OL_AMOUNT uint64 = 17
const X_OL_LEN uint64 = 21

type Item struct {
	/* Primary key: I_ID */
	I_ID uint32
	/* Data fields */
	I_IM_ID uint32
	I_NAME  [24]byte
	I_PRICE float32
	I_DATA  [50]byte
}

const X_I_ID uint64 = 0
const X_I_IM_ID uint64 = 4
const X_I_NAME uint64 = 8
const X_I_PRICE uint64 = 32
const X_I_DATA uint64 = 36
const X_I_LEN uint64 = 86

type Stock struct {
	/* Primary key: (S_W_ID, S_I_ID) */
	S_I_ID uint32
	S_W_ID uint8
	/* Data fields */
	S_QUANTITY   uint16
	S_YTD        uint32
	S_ORDER_CNT  uint16
	S_REMOTE_CNT uint16
	S_DATA       [50]byte
}

const X_S_I_ID uint64 = 0
const X_S_W_ID uint64 = 4
const X_S_QUANTITY uint64 = 5
const X_S_YTD uint64 = 7
const X_S_ORDER_CNT uint64 = 11
const X_S_REMOTE_CNT uint64 = 13
const X_S_DATA uint64 = 15
const X_S_LEN uint64 = 65

const (
	/* Tables. */
	TBLID_WAREHOUSE uint64 = iota << 56
	TBLID_DISTRICT
	TBLID_CUSTOMER
	TBLID_HISTORY
	TBLID_NEWORDER
	TBLID_ORDER
	TBLID_ORDERLINE
	TBLID_ITEM
	TBLID_STOCK
	/* Index. */
	/* ORDER index on (O_W_ID, O_D_ID, O_C_ID). */
	IDXID_ORDER
)

/* NULL values. */
const O_CARRIER_ID_NULL uint8 = 0xff
const OL_DELIVERY_D_NULL uint32 = 0xffffffff

/**
 * Input definitions.
 */
type NewOrderInput struct {
	W_ID      uint8
	D_ID      uint8
	C_ID      uint32
	O_ENTRY_D uint32
	I_IDS     []uint32
	I_W_IDS   []uint8
	I_QTYS    []uint8
}

type PaymentInput struct {
	W_ID     uint8
	D_ID     uint8
	H_AMOUNT float32
	C_W_ID   uint8
	C_D_ID   uint8
	C_ID     uint32
	H_DATE   uint32
}

type OrderStatusInput struct {
	W_ID uint8
	D_ID uint8
	C_ID uint32
}

type DeliveryInput struct {
	W_ID          uint8
	O_CARRIER_ID  uint8
	OL_DELIVERY_D uint32
}

type StockLevelInput struct {
	W_ID      uint8
	D_ID      uint8
	THRESHOLD uint16
}
