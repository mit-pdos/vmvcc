package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
	"github.com/mit-pdos/go-mvcc/txn"
)

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
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a Customer record.
 * Used by TableRead.
 */
func (x *Customer) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
