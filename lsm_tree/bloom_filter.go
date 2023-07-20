package LsmTree

import (
	"hash"
	"math"
	"math/rand"
	"sync"

	"github.com/twmb/murmur3"
)

const BLOOM_FILTER_MAX_LEN = 1000000
const DEFAULT_ERROR_RATE = 0.01
const DEFAULT_FLUSH_THRESHOLD = 0.8

type BloomParameters struct {
	capacity  int
	hashFns   []hash.Hash64
	bpe       float64
	numOfBits int
}

type BloomFilter struct {
	bloomParameters BloomParameters
	hashRWLock      []sync.RWMutex
	bloomLock       sync.RWMutex
	bistset         []uint64
}

type BloomFilterOpts struct {
	Capacity  int
	ErrorRate float64
}

func CreateBloomFilter(opts BloomFilterOpts) *BloomFilter {
	capacity := opts.Capacity
	errorRate := opts.ErrorRate

	var bloomOpts BloomParameters
	bloomOpts.capacity = capacity

	bloomOpts.bpe = -1 * math.Log(errorRate) / math.Pow(math.Log(2), 2)

	k := math.Ceil(bloomOpts.bpe * math.Log(2))
	bloomOpts.hashFns = make([]hash.Hash64, int(k))

	for i := 0; i < int(k); i++ {
		bloomOpts.hashFns[i] = murmur3.SeedNew64(rand.Uint64())
	}

	bitset := make([]uint64, BLOOM_FILTER_MAX_LEN)
	bloomOpts.numOfBits = BLOOM_FILTER_MAX_LEN * 64

	b := &BloomFilter{
		bloomParameters: bloomOpts,
		bistset:         bitset,
		hashRWLock:      make([]sync.RWMutex, int(k)),
	}

	return b
}

func (b *BloomFilter) Add(key string) {

	for i := 0; i < len(b.bloomParameters.hashFns); i++ {
		b.hashRWLock[i].Lock()
		b.bloomParameters.hashFns[i].Reset()
		b.bloomParameters.hashFns[i].Write([]byte(key))
		hashValue := b.bloomParameters.hashFns[i].Sum64() % uint64(b.bloomParameters.numOfBits)
		setBit(&b, hashValue)
		b.hashRWLock[i].Unlock()

	}
}

func (b *BloomFilter) Contains(key string) bool {

	for i := 0; i < len(b.bloomParameters.hashFns); i++ {
		b.hashRWLock[i].Lock()
		b.bloomParameters.hashFns[i].Reset()

		b.bloomParameters.hashFns[i].Write([]byte(key))
		hashValue := b.bloomParameters.hashFns[i].Sum64() % uint64(b.bloomParameters.numOfBits)

		if !hasBit(&b, hashValue) {
			b.hashRWLock[i].Unlock()
			return false
		}
		b.hashRWLock[i].Unlock()
	}

	return true
}

func hasBit(b **BloomFilter, bitIndex uint64) bool {
	return (*b).bistset[bitIndex>>6]&(1<<uint(bitIndex%64)) != 0
}

func setBit(b **BloomFilter, bitIndex uint64) {
	(*b).bistset[bitIndex>>6] |= (1 << uint(bitIndex%64))
}
