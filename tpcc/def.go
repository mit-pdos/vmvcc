package tpcc

/* Customer table. */
type Customer struct {
	C_ID   uint64
	C_W_ID uint64
	/* Unique key: (C_ID, C_W_ID). */
	C_LAST []byte
	/* Non-unique key: (C_LAST, C_W_ID). */
}

const (
	MAPID_CUSTOMER_TBL      uint64 = 0
	MAPID_CUSTOMER_IDX_CID  uint64 = 1
	MAPID_CUSTOMER_IDX_LAST uint64 = 2
)
