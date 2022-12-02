package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
)

func NewItem(iid uint32) *Item {
	x := Item { I_ID : iid }
	return &x
}

/**
 * Table mutator methods.
 */
func (x *Item) Initiailize(
	imid uint32, name string, price float32, data string,
) {
	x.I_IM_ID = imid
	copy(x.I_NAME[:], name)
	x.I_PRICE = price
	copy(x.I_DATA[:], data)
}

/**
 * Convert primary keys of table Item to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *Item) gkey() uint64 {
	var gkey uint64 = uint64(x.I_ID)
	gkey += TBLID_ITEM
	return gkey
}

/**
 * Encode a Item record to an opaque string.
 * Used by TableWrite.
 */
func (x *Item) encode() string {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a Item record.
 * Used by TableRead.
 */
func (x *Item) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
