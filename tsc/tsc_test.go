package tsc

import (
	"testing"
	"fmt"
)

func TestGetTSC(t *testing.T) {
	tsc := GetTSC()
	fmt.Printf("tsc = %d.\n", tsc)

	tsc = GetTSC()
	fmt.Printf("tsc = %d.\n", tsc)
}

