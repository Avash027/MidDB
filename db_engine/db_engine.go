package dbengine

import (
	diskstore "github.com/Avash027/midDB/disk_store"
	LsmTree "github.com/Avash027/midDB/lsm_tree"
	"github.com/Avash027/midDB/wal"
)

type DBEngine struct {
	LsmTree *LsmTree.LSMTree
	Wal     *wal.WAL
	Store   *diskstore.DiskStore
}

func (db *DBEngine) LoadFromDisk(lsmTree *LsmTree.LSMTree, wal *wal.WAL) error {
	return db.Store.LoadFromDisk(lsmTree, wal)
}
