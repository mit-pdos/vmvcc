package tpcc

import (
	"github.com/mit-pdos/go-mvcc/txn"
)

func GetDistrict(txn *txn.Txn, did uint8, wid uint8) (*District, bool) {
	x := &District {
		D_ID   : did,
		D_W_ID : wid,
	}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

func InsertDistrict(
	txn *txn.Txn,
	did uint8, wid uint8,
	name, street1, street2, city string,
	state [2]byte, zip [9]byte, tax, ytd float32,
	nextoid, oldestoid uint32,
) {
	x := &District {
		D_ID        : did,
		D_W_ID      : wid,
		D_STATE     : state,
		D_ZIP       : zip,
		D_TAX       : tax,
		D_YTD       : ytd,
		D_NEXT_O_ID : nextoid,
		D_OLD_O_ID  : oldestoid,
	}
	copy(x.D_NAME[:], name)
	copy(x.D_STREET_1[:], street1)
	copy(x.D_STREET_2[:], street2)
	copy(x.D_CITY[:], city)
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func (x *District) IncrementNextOrderId(txn *txn.Txn) {
	x.D_NEXT_O_ID++
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func (x *District) IncrementOldestOrderId(txn *txn.Txn) {
	x.D_OLD_O_ID++
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func (x *District) UpdateBalance(txn *txn.Txn, hamount float32) {
	x.D_YTD += hamount
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Convert primary keys of table District to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *District) gkey() uint64 {
	var gkey uint64 = uint64(x.D_ID)
	gkey = gkey << 8 + uint64(x.D_W_ID)
	gkey += TBLID_DISTRICT
	return gkey
}

/**
 * Encode a District record to an opaque string.
 * Used by TableWrite.
 */
func (x *District) encode() string {
	buf := make([]byte, X_D_LEN)
	encodeU8(buf, x.D_ID, X_D_ID)
	encodeU8(buf, x.D_W_ID, X_D_W_ID)
	encodeBytes(buf, x.D_NAME[:], X_D_NAME)
	encodeBytes(buf, x.D_STREET_1[:], X_D_STREET_1)
	encodeBytes(buf, x.D_STREET_2[:], X_D_STREET_2)
	encodeBytes(buf, x.D_CITY[:], X_D_CITY)
	encodeBytes(buf, x.D_STATE[:], X_D_STATE)
	encodeBytes(buf, x.D_ZIP[:], X_D_ZIP)
	encodeF32(buf, x.D_TAX, X_D_TAX)
	encodeF32(buf, x.D_YTD, X_D_YTD)
	encodeU32(buf, x.D_NEXT_O_ID, X_D_NEXT_O_ID)
	encodeU32(buf, x.D_OLD_O_ID, X_D_OLD_O_ID)
	return string(buf)
}

/**
 * Decode an opaque string to a District record.
 * Used by TableRead.
 */
func (x *District) decode(opaque string) {
	decodeU8(&x.D_ID, opaque, X_D_ID)
	decodeU8(&x.D_W_ID, opaque, X_D_W_ID)
	decodeString(x.D_NAME[:], opaque, X_D_NAME)
	decodeString(x.D_STREET_1[:], opaque, X_D_STREET_1)
	decodeString(x.D_STREET_2[:], opaque, X_D_STREET_2)
	decodeString(x.D_CITY[:], opaque, X_D_CITY)
	decodeString(x.D_STATE[:], opaque, X_D_STATE)
	decodeString(x.D_ZIP[:], opaque, X_D_ZIP)
	decodeF32(&x.D_TAX, opaque, X_D_TAX)
	decodeF32(&x.D_YTD, opaque, X_D_YTD)
	decodeU32(&x.D_NEXT_O_ID, opaque, X_D_NEXT_O_ID)
	decodeU32(&x.D_OLD_O_ID, opaque, X_D_OLD_O_ID)
}
