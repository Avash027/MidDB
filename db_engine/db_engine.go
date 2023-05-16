package dbengine

import (
	LsmTree "github.com/Avash027/midDB/lsm_tree"
	"github.com/Avash027/midDB/wal"
)

type DBEngine struct {
	LsmTree *LsmTree.LSMTree
	Wal     *wal.WAL
}
