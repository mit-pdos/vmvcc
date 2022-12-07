package ycsb

import (
	"math/rand"
)

const (
	OP_RD = iota
	OP_WR
)

const (
	DIST_UNIFORM = iota
	DIST_ZIPFIAN
)

type Generator struct {
	rd      *rand.Rand
	nKeys   int
	rKeys   uint64
	rdRatio uint64
	dist    int
}

func NewGenerator(
	seed int,
	nKeys int, rKeys, rdRatio uint64,
	dist int,
) *Generator {
	rd := rand.New(rand.NewSource(int64(seed)))

	gen := &Generator {
		rd : rd,
		nKeys : nKeys,
		rKeys : rKeys,
		rdRatio : rdRatio,
		dist : dist,
	}

	return gen
}

func (g *Generator) PickOp() int {
	x := g.rd.Uint64() % 100
	if x < g.rdRatio {
		return OP_RD
	} else {
		return OP_WR
	}
}

func (g *Generator) pickKeyUniform() uint64 {
	return g.rd.Uint64() % g.rKeys
}

func (g *Generator) PickKey() uint64 {
	return g.pickKeyUniform()
}

func (g *Generator) NKeys() int {
	return g.nKeys
}
