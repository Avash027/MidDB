package LsmTree

import (
	"sync"
	"time"
)

const DEFAULT_MAX_ELEMENTS_BEFORE_FLUSH = 1024
const DEFAULT_COMPACTION_FREQUENCY = 1000
const DEFAULT_BLOOM_FILTER_ERROR_RATE = 0.0001
const DEFAULT_BLOOM_FILTER_CAPACITY = 1000000

type Pair struct {
	Key       string
	Value     string
	Tombstone bool
}

type LSMTree struct {
	treereadWriteLock      sync.RWMutex
	diskReadWriteLock      sync.RWMutex
	tree                   *TreeNode
	secondaryTree          *TreeNode
	diskBlocks             []DiskBlock
	MaxElementsBeforeFlush int
	BloomFilter            *BloomFilter
}

type LSMTreeOpts struct {
	MaxElementsBeforeFlush int
	CompactionPeriod       int
	BloomFilterOpts        BloomFilterOpts
}

func InitNewLSMTree(opts LSMTreeOpts) *LSMTree {
	lsmTree := &LSMTree{
		tree:                   &TreeNode{},
		secondaryTree:          &TreeNode{},
		diskBlocks:             []DiskBlock{},
		MaxElementsBeforeFlush: opts.MaxElementsBeforeFlush,
		BloomFilter:            CreateBloomFilter(opts.BloomFilterOpts),
	}

	go lsmTree.PeriodicCompaction(opts.CompactionPeriod)
	return lsmTree

}

func (lsmTree *LSMTree) PeriodicCompaction(compactionPeriod int) {

	for {
		time.Sleep(time.Duration(compactionPeriod) * time.Millisecond)
		var db1, db2 DiskBlock

		lsmTree.diskReadWriteLock.RLock()

		if len(lsmTree.diskBlocks) >= 2 {
			db1 = lsmTree.diskBlocks[len(lsmTree.diskBlocks)-1]
			db2 = lsmTree.diskBlocks[len(lsmTree.diskBlocks)-2]
		}

		if db1.Empty() || db2.Empty() {
			continue
		}

		newDiskBlock := compact(db1, db2)

		lsmTree.diskBlocks = lsmTree.diskBlocks[0 : len(lsmTree.diskBlocks)-2]
		lsmTree.diskBlocks = append(lsmTree.diskBlocks, newDiskBlock)
		lsmTree.diskReadWriteLock.RUnlock()

	}
}

func compact(db1 DiskBlock, db2 DiskBlock) DiskBlock {
	pairs1 := db1.All()
	pairs2 := db2.All()

	// merge the two arrays in the increasing order of key values
	i, j := 0, 0
	var newPairs []Pair

	for i < len(pairs1) && j < len(pairs2) {
		if pairs1[i].Key < pairs2[j].Key {
			newPairs = append(newPairs, pairs1[i])
			i++
		} else {
			newPairs = append(newPairs, pairs2[j])
			j++
		}
	}

	for i < len(pairs1) {
		newPairs = append(newPairs, pairs1[i])
		i++
	}

	for j < len(pairs2) {
		newPairs = append(newPairs, pairs2[j])
		j++
	}

	return NewDiskBlock(newPairs)

}

func (lsmTree *LSMTree) Get(key string) (string, bool) {

	lsmTree.treereadWriteLock.RLock()

	pair, err := lsmTree.tree.Find(key)

	if err == nil {

		lsmTree.treereadWriteLock.RUnlock()
		if pair.Tombstone {
			return "", false
		}

		return pair.Value, true
	}

	pair, err = lsmTree.secondaryTree.Find(key)

	if err == nil {

		lsmTree.treereadWriteLock.RUnlock()
		if pair.Tombstone {
			return "", false
		}
		return pair.Value, true
	}

	exist := lsmTree.BloomFilter.Contains(key)

	if !exist {
		return "", false
	}

	lsmTree.treereadWriteLock.RUnlock()
	lsmTree.diskReadWriteLock.RLock()
	defer lsmTree.diskReadWriteLock.RUnlock()

	for _, diskBlock := range lsmTree.diskBlocks {
		pair, err = diskBlock.GetDataFromDiskBlock(key)
		if err == nil {

			if pair.Tombstone {
				continue
			}
			return pair.Value, true
		}
	}

	return "", false
}

func (lsmTree *LSMTree) Put(key string, value string) {
	lsmTree.treereadWriteLock.Lock()
	defer lsmTree.treereadWriteLock.Unlock()

	Insert(&(lsmTree.tree), Pair{key, value, false})

	go lsmTree.BloomFilter.Add(key)

	if lsmTree.tree.GetSize() >= lsmTree.MaxElementsBeforeFlush && lsmTree.secondaryTree == nil {

		lsmTree.secondaryTree = lsmTree.tree
		lsmTree.tree = nil
		go lsmTree.Flush()
	}
}

func (lsmTree *LSMTree) Del(key string) {
	lsmTree.treereadWriteLock.Lock()
	defer lsmTree.treereadWriteLock.Unlock()

	Delete(&(lsmTree.tree), key)
	Delete(&(lsmTree.secondaryTree), key)

	for _, diskBlock := range lsmTree.diskBlocks {
		err := diskBlock.Del(key)
		if err == nil {
			break
		}
	}

}

func (LSMTree *LSMTree) Flush() {
	newDiskBlocks := []DiskBlock{NewDiskBlock(LSMTree.secondaryTree.All())}

	LSMTree.diskReadWriteLock.Lock()
	LSMTree.diskBlocks = append(LSMTree.diskBlocks, newDiskBlocks...)
	LSMTree.diskReadWriteLock.Unlock()

	LSMTree.treereadWriteLock.Lock()
	LSMTree.secondaryTree = nil
	LSMTree.treereadWriteLock.Unlock()
}
