package main

import (
	"math/rand"
	/* for Zipfian distribution */
	"github.com/pingcap/go-ycsb/pkg/generator"
)

const (
	OP_RD = iota
	OP_WR
	OP_SCAN
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
	zipfian *generator.Zipfian
	dist    int
}

func NewGenerator(
	seed int,
	nKeys int, rKeys, rdRatio uint64,
	theta float64,
) *Generator {
	rd := rand.New(rand.NewSource(int64(seed)))

	var zipfian *generator.Zipfian
	var dist int
	if theta == -1 {
		dist = DIST_UNIFORM
	} else {
		dist = DIST_ZIPFIAN
		zipfian = generator.NewZipfianWithItems(int64(rKeys), theta)
	}

	gen := &Generator {
		rd : rd,
		nKeys : nKeys,
		rKeys : rKeys,
		rdRatio : rdRatio,
		zipfian : zipfian,
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

func (g *Generator) pickKeyZipfian() uint64 {
	return uint64(g.zipfian.Next(g.rd))
}

func (g *Generator) PickKey() uint64 {
	if g.dist == DIST_ZIPFIAN {
		return g.pickKeyZipfian()
	} else {
		return g.pickKeyUniform()
	}
}

func (g *Generator) NKeys() int {
	return g.nKeys
}
