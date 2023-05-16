package wal

import (
	"bufio"
	"os"
	"sync"
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
	lock   sync.Mutex
}

func InitWAL(path string) *WAL {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(file)

	return &WAL{file: file, writer: writer}
}

func (w *WAL) Write(data []byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	_, err := w.writer.Write(data)

	if err != nil {
		return err
	}

	return nil
}

func (w *WAL) Flush() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	err := w.writer.Flush()

	return err
}
