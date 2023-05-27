package DiskStore

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	LsmTree "github.com/Avash027/midDB/lsm_tree"
	"github.com/Avash027/midDB/wal"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrCASConflict = errors.New("compare-and-swap conflict")
)

const DEFAULT_NUM_OF_PARTITIONS = 10
const DEFAULT_DIRECTORY = "./data"

type DiskStoreOpts struct {
	Directory       string
	NumOfPartitions int
}

type DiskStore struct {
	files []*os.File
	dir   string
	Locks []*sync.RWMutex
	Lock  sync.Mutex
}

func New(opts DiskStoreOpts) *DiskStore {

	dir := opts.Directory
	if !strings.HasSuffix(dir, "/") {
		dir = dir + "/"
	}

	numOfPartitions := opts.NumOfPartitions

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil
	}

	ds := &DiskStore{
		dir:   dir,
		files: make([]*os.File, numOfPartitions),
		Locks: make([]*sync.RWMutex, numOfPartitions),
		Lock:  sync.Mutex{},
	}

	for i := 0; i < numOfPartitions; i++ {
		filename := fmt.Sprintf("%s/partition_%d", dir, i)
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			// Handle error and cleanup previously created files
			for j := 0; j < i; j++ {
				ds.files[j].Close()
				os.Remove(ds.files[j].Name())
			}
			return nil
		}
		ds.files[i] = file
		ds.Locks[i] = &sync.RWMutex{}
	}

	return ds
}

func partition(key string, numPartitions int) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32() % uint32(numPartitions))
}

func (ds *DiskStore) PersistToDisk(wl *wal.WAL, start <-chan bool) {
	<-start

	fmt.Println("Starting persisting cycle")
	for {

		ds.Lock.Lock()
		var wg sync.WaitGroup
		entries := wl.ReadEntries()
		wg.Add(len(entries))

		for _, entry := range entries {
			go func(entry wal.Entry, wg *sync.WaitGroup) {
				partition := partition(entry.Key, len(ds.files))
				file := ds.files[partition]

				defer (*wg).Done()

				existingValue, err := ds.readValue(file, entry.Key, partition)
				if err != nil {
					fmt.Println(err)
					existingValue = nil
				}

				if entry.Delete {
					err = ds.DeleteFromDisk(file, entry.Key, partition)
					if err != nil {
						fmt.Printf("%s", err.Error())
					}
					return
				}

				err = ds.writeValue(file, entry.Key, []byte(entry.Value), existingValue, partition)
				if err != nil {
					fmt.Printf("%s", err.Error())
				}
			}(entry, &wg)
		}

		wg.Wait()
		wl.Truncate()
		ds.Lock.Unlock()

		time.Sleep(5 * time.Second)

	}
}

func (ds *DiskStore) readValue(file *os.File, key string, partition int) ([]byte, error) {
	// Seek to the beginning of the file
	ds.Locks[partition].Lock()
	defer ds.Locks[partition].Unlock()

	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Scan the file for the key
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if parts[0] == key {
			return []byte(parts[1]), nil
		}
	}

	// If the key wasn't found, return nil
	return nil, nil
}

func (ds *DiskStore) writeValue(file *os.File, key string, value []byte, existingValue []byte, partition int) error {
	ds.Locks[partition].RLock()
	defer ds.Locks[partition].RUnlock()

	// Seek to the beginning of the file
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	// Write the new key-value pair to a buffer
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s:%s\n", key, value))

	// If there was an existing value, replace it in the buffer
	if existingValue != nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, ":")
			if parts[0] == key {
				continue
			}
			buf.WriteString(line + "\n")
		}
	}

	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	_, err = file.Write(buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (ds *DiskStore) DeleteFromDisk(file *os.File, key string, partition int) error {
	ds.Locks[partition].RLock()
	defer ds.Locks[partition].RUnlock()

	_, err := file.Seek(0, io.SeekStart)

	if err != nil {
		return err
	}

	var buf bytes.Buffer
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if parts[0] == key {
			continue
		}
		buf.WriteString(line + "\n")
	}

	if err := file.Truncate(0); err != nil {
		return err
	}

	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	_, err = file.Write(buf.Bytes())

	if err != nil {
		return err
	}

	return nil
}

func (ds *DiskStore) GetFileContents(i int) []wal.Entry {
	ds.Locks[i].RLock()
	defer ds.Locks[i].RUnlock()

	_, err := ds.files[i].Seek(0, io.SeekStart)
	if err != nil {
		return nil
	}

	scanner := bufio.NewScanner(ds.files[i])
	var entries []wal.Entry
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		entries = append(entries, wal.Entry{
			Key:   parts[0],
			Value: parts[1],
		})
	}

	return entries
}

func (ds *DiskStore) LoadFromDisk(lsmTree *LsmTree.LSMTree, wal *wal.WAL) error {

	for i := 0; i < len(ds.files); i++ {
		entries := ds.GetFileContents(i)

		for _, entry := range entries {
			lsmTree.Put(entry.Key, entry.Value)
		}
	}

	err := wal.InitDB(lsmTree)

	if err != nil {

		return err
	}

	return nil

}
