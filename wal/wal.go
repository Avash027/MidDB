package wal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	LsmTree "github.com/Avash027/midDB/lsm_tree"
)

type Entry struct {
	Key    string `json:"k"`
	Value  string `json:"v"`
	Delete bool   `json:"-"`
}

const DEFAULT_WAL_PATH = "wal.aof"

type WAL struct {
	filepath string
	File     *os.File
	writer   *bufio.Writer
	lock     sync.Mutex
}

func InitWAL(path string) *WAL {
	var file *os.File

	if _, err := os.Stat(path); os.IsNotExist(err) {
		file, err = os.Create(path)
		if err != nil {
			panic(err)
		}
	} else {
		file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
	}

	writer := bufio.NewWriter(file)

	return &WAL{filepath: path, File: file, writer: writer}
}

func (w *WAL) Write(data ...[]byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	// if the size of incoming data is more than the available buffer size
	// then flush the buffer to the file
	if len(data) > w.writer.Available() {
		if err := w.writer.Flush(); err != nil {
			fmt.Println(err)
			return err
		}
	}

	endByte := []byte("\n")
	delimiter := []byte("|")

	for _, d := range data {
		d = append(d, delimiter...)
		_, err := w.writer.Write(d)
		if err != nil {
			return err
		}
	}

	w.writer.WriteString(string(endByte))

	return nil
}

func (w *WAL) Persist() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}

	if err := w.File.Sync(); err != nil {
		return err
	}

	// clear the write buffer
	w.writer.Reset(w.File)

	return nil
}

func (w *WAL) ReadEntries() []Entry {
	w.lock.Lock()
	defer w.lock.Unlock()

	file, err := os.OpenFile(w.filepath, os.O_RDONLY, 0644)

	if err != nil {
		panic(err)
	}

	reader := bufio.NewReader(file)

	if err != nil {
		panic(err)
	}

	data, err := io.ReadAll(reader)

	if err != nil {
		panic(err)
	}

	cmds := strings.Split(string(data), "\n")

	entries := make([]Entry, 0, len(cmds))

	for _, cmd := range cmds {
		if cmd == "" {
			continue
		}

		args := strings.Split(cmd, "|")

		switch args[0] {
		case "+":
			if len(args) != 4 {
				continue
			}
			entries = append(entries, Entry{Key: args[1], Value: args[2], Delete: false})
		case "-":
			if len(args) != 3 {
				continue
			}
			entries = append(entries, Entry{Key: args[1], Delete: true})
		}
	}

	return entries
}

func (w *WAL) InitDB(lsmTree *LsmTree.LSMTree) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	file, err := os.OpenFile(w.filepath, os.O_RDONLY, 0644)

	if err != nil {
		return err
	}

	reader := bufio.NewReader(file)

	if err != nil {
		return err
	}

	data, err := io.ReadAll(reader)

	if err != nil {
		return err
	}

	cmds := strings.Split(string(data), "\n")

	for _, cmd := range cmds {
		if cmd == "" {
			continue
		}

		args := strings.Split(cmd, "|")

		switch args[0] {
		case "+":
			lsmTree.Put(args[1], args[2])
		case "-":
			lsmTree.Del(args[1])
		}
	}

	return nil
}

func (w *WAL) Truncate() {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.File.Truncate(0)
	w.File.Seek(0, 0)
}
