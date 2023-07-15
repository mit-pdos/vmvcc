package tpcc

import (
	"github.com/mit-pdos/vmvcc/txn"
)

func GetCustomerX(
	txn *txn.Txn,
	cid uint32, did uint8, wid uint8, x *Customer,
) bool {
	x.C_ID = cid
	x.C_D_ID = did
	x.C_W_ID = wid
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return found
}

func GetCustomer(
	txn *txn.Txn,
	cid uint32, did uint8, wid uint8,
) (*Customer, bool) {
	x := &Customer {
		C_ID   : cid,
		C_D_ID : did,
		C_W_ID : wid,
	}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

/**
 * Table mutator methods.
 */
func InsertCustomer(
	txn *txn.Txn,
	cid uint32, did uint8, wid uint8,
	first string, middle [2]byte, last, street1, street2, city string,
	state [2]byte, zip [9]byte, phone [16]byte, since uint32,
	credit [2]byte, creditlim, discount, balance, payment float32,
	pcnt, dcnt uint16, data string,
) {
	x := &Customer {
		C_ID          : cid,
		C_D_ID        : did,
		C_W_ID        : wid,
		C_MIDDLE      : middle,
		C_STATE       : state,
		C_ZIP         : zip,
		C_PHONE       : phone,
		C_SINCE       : since,
		C_CREDIT      : credit,
		C_CREDIT_LIM  : creditlim,
		C_DISCOUNT    : discount,
		C_BALANCE     : balance,
		C_YTD_PAYMENT : payment,
		C_PAYMENT_CNT : pcnt,
		C_DELIVERY_CNT: dcnt,
	}
	copy(x.C_FIRST[:], first)
	copy(x.C_LAST[:], last)
	copy(x.C_STREET_1[:], street1)
	copy(x.C_STREET_2[:], street2)
	copy(x.C_CITY[:], city)
	copy(x.C_DATA[:], data)
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func (x *Customer) UpdateOnBadCredit(
	txn *txn.Txn,
	hamount float32, cdata string,
) {
	x.C_BALANCE -= hamount
	x.C_YTD_PAYMENT += hamount
	x.C_PAYMENT_CNT++
	copy(x.C_DATA[:], cdata)
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func (x *Customer) UpdateOnGoodCredit(txn *txn.Txn, hamount float32) {
	x.C_BALANCE -= hamount
	x.C_YTD_PAYMENT += hamount
	x.C_PAYMENT_CNT++
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func (x *Customer) IncreaseBalance(txn *txn.Txn, total float32) {
	x.C_BALANCE += total
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Convert primary keys of table Customer to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *Customer) gkey() uint64 {
	var gkey uint64 = uint64(x.C_ID)
	gkey = gkey << 8 + uint64(x.C_D_ID)
	gkey = gkey << 8 + uint64(x.C_W_ID)
	gkey += TBLID_CUSTOMER
	return gkey
}

/**
 * Encode a Customer record to an opaque string.
 * Used by TableWrite.
 */
func (x *Customer) encode() string {
	buf := make([]byte, X_C_LEN)
	encodeU32(buf, x.C_ID, X_C_ID)
	encodeU8(buf, x.C_D_ID, X_C_D_ID)
	encodeU8(buf, x.C_W_ID, X_C_W_ID)
	encodeBytes(buf, x.C_FIRST[:], X_C_FIRST)
	encodeBytes(buf, x.C_MIDDLE[:], X_C_MIDDLE)
	encodeBytes(buf, x.C_LAST[:], X_C_LAST)
	encodeBytes(buf, x.C_STREET_1[:], X_C_STREET_1)
	encodeBytes(buf, x.C_STREET_2[:], X_C_STREET_2)
	encodeBytes(buf, x.C_CITY[:], X_C_CITY)
	encodeBytes(buf, x.C_STATE[:], X_C_STATE)
	encodeBytes(buf, x.C_ZIP[:], X_C_ZIP)
	encodeBytes(buf, x.C_PHONE[:], X_C_PHONE)
	encodeU32(buf, x.C_SINCE, X_C_SINCE)
	encodeBytes(buf, x.C_CREDIT[:], X_C_CREDIT)
	encodeF32(buf, x.C_CREDIT_LIM, X_C_CREDIT_LIM)
	encodeF32(buf, x.C_DISCOUNT, X_C_DISCOUNT)
	encodeF32(buf, x.C_BALANCE, X_C_BALANCE)
	encodeF32(buf, x.C_YTD_PAYMENT, X_C_YTD_PAYMENT)
	encodeU16(buf, x.C_PAYMENT_CNT, X_C_PAYMENT_CNT)
	encodeU16(buf, x.C_DELIVERY_CNT, X_C_DELIVERY_CNT)
	encodeBytes(buf, x.C_DATA[:], X_C_DATA)
	return bytesToString(buf)
}

/**
 * Decode an opaque string to a Customer record.
 * Used by TableRead.
 */
func (x *Customer) decode(opaque string) {
	decodeU32(&x.C_ID, opaque, X_C_ID)
	decodeU8(&x.C_D_ID, opaque, X_C_D_ID)
	decodeU8(&x.C_W_ID, opaque, X_C_W_ID)
	decodeString(x.C_FIRST[:], opaque, X_C_FIRST)
	decodeString(x.C_MIDDLE[:], opaque, X_C_MIDDLE)
	decodeString(x.C_LAST[:], opaque, X_C_LAST)
	decodeString(x.C_STREET_1[:], opaque, X_C_STREET_1)
	decodeString(x.C_STREET_2[:], opaque, X_C_STREET_2)
	decodeString(x.C_CITY[:], opaque, X_C_CITY)
	decodeString(x.C_STATE[:], opaque, X_C_STATE)
	decodeString(x.C_ZIP[:], opaque, X_C_ZIP)
	decodeString(x.C_PHONE[:], opaque, X_C_PHONE)
	decodeU32(&x.C_SINCE, opaque, X_C_SINCE)
	decodeString(x.C_CREDIT[:], opaque, X_C_CREDIT)
	decodeF32(&x.C_CREDIT_LIM, opaque, X_C_CREDIT_LIM)
	decodeF32(&x.C_DISCOUNT, opaque, X_C_DISCOUNT)
	decodeF32(&x.C_BALANCE, opaque, X_C_BALANCE)
	decodeF32(&x.C_YTD_PAYMENT, opaque, X_C_YTD_PAYMENT)
	decodeU16(&x.C_PAYMENT_CNT, opaque, X_C_PAYMENT_CNT)
	decodeU16(&x.C_DELIVERY_CNT, opaque, X_C_DELIVERY_CNT)
	decodeString(x.C_DATA[:], opaque, X_C_DATA)
}
