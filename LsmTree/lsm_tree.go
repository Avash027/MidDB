package LsmTree

import (
	"fmt"
	"sync"
	"time"

	"github.com/Avash027/midDB/logger"
)

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
}

func InitNewLSMTree(maxElementsBeforeFlush int, compactionPeriod int) *LSMTree {
	lsmTree := &LSMTree{
		tree:                   &TreeNode{},
		secondaryTree:          &TreeNode{},
		diskBlocks:             []DiskBlock{},
		MaxElementsBeforeFlush: maxElementsBeforeFlush,
	}

	go lsmTree.PeriodicCompaction(compactionPeriod)
	return lsmTree

}

func (lsmTree *LSMTree) PeriodicCompaction(compactionPeriod int) {

	for {
		time.Sleep(time.Duration(compactionPeriod) * time.Second)
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

	logs := logger.GetLogger()

	logs.Debug().Msg(fmt.Sprintf("Compacting %v and %v", pairs1, pairs2))

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
	logs := logger.GetLogger()
	lsmTree.treereadWriteLock.RLock()

	pair, err := lsmTree.tree.Find(key)

	if err == nil {
		logs.Debug().Msg(fmt.Sprintf("Found %v in tree", pair))
		lsmTree.treereadWriteLock.RUnlock()
		if pair.Tombstone {
			return "", false
		}

		return pair.Value, true
	}

	pair, err = lsmTree.secondaryTree.Find(key)

	if err == nil {
		logs.Debug().Msg(fmt.Sprintf("Found %v in secondary tree", pair))
		lsmTree.treereadWriteLock.RUnlock()
		if pair.Tombstone {
			return "", false
		}
		return pair.Value, true
	}

	lsmTree.treereadWriteLock.RUnlock()
	lsmTree.diskReadWriteLock.RLock()
	defer lsmTree.diskReadWriteLock.RUnlock()

	for _, diskBlock := range lsmTree.diskBlocks {
		pair, err = diskBlock.GetDataFromDiskBlock(key)
		if err == nil {
			logs.Debug().Msg(fmt.Sprintf("Found %v in disk block", pair))
			if pair.Tombstone {
				continue
			}
			return pair.Value, true
		}
	}

	return "", false
}

func (lsmTree *LSMTree) Put(key string, value string) {
	logs := logger.GetLogger()
	lsmTree.treereadWriteLock.Lock()
	defer lsmTree.treereadWriteLock.Unlock()

	Insert(&(lsmTree.tree), Pair{key, value, false})

	if lsmTree.tree.GetSize() >= lsmTree.MaxElementsBeforeFlush && lsmTree.secondaryTree == nil {
		logs.Debug().Msg("Flushing tree because it exceeded max elements threshold")
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
