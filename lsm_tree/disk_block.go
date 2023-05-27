package LsmTree

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"strconv"
)

const (
	MAX_ELEMENTS_IN_DISK_BLOCK = 1024
	INDEX_RATIO                = 10
)

type DiskBlock struct {
	index         *TreeNode
	NumOfElements int
	buffer        bytes.Buffer
}

func NewDiskBlock(elements []Pair) DiskBlock {
	diskBlock := DiskBlock{NumOfElements: len(elements)}
	indexElements := make([]Pair, 0)
	var encoder *gob.Encoder

	for i, element := range elements {
		if i%INDEX_RATIO == 0 {
			idx := Pair{Key: element.Key, Value: fmt.Sprintf("%d", diskBlock.buffer.Len())}
			indexElements = append(indexElements, idx)
			encoder = gob.NewEncoder(&diskBlock.buffer)
		}
		encoder.Encode(element)
	}
	diskBlock.index = NewTreeNode(indexElements)
	return diskBlock

}

func (d *DiskBlock) GetDataFromDiskBlock(key string) (Pair, error) {
	if d.Empty() {
		return Pair{}, fmt.Errorf("DiskBlock is empty")
	}

	start_, err := d.index.GreatestKeyLessThanOrEqualTo(key)

	if err != nil {
		return Pair{}, err
	}

	startIndex, _ := strconv.Atoi(start_.Value)

	end_, err := d.index.SmallestKeyGreaterThan(key)

	if err != nil {
		return Pair{}, err
	}

	endIndex, _ := strconv.Atoi(end_.Value)

	searchBuffer := bytes.NewBuffer(d.buffer.Bytes()[startIndex:endIndex])
	dec := gob.NewDecoder(searchBuffer)

	for {
		var pair Pair
		if err := dec.Decode(&pair); err == io.EOF {
			break
		} else if err != nil {
			return Pair{}, err
		}

		if pair.Key == key {
			return pair, nil
		}
	}

	return Pair{}, fmt.Errorf("key not found")

}

func (d *DiskBlock) All() []Pair {
	indexElems := d.index.All()
	var pairs []Pair

	for i, indexElem := range indexElems {
		startIndex, _ := strconv.Atoi(indexElem.Value)
		endIndex := d.buffer.Len()

		if i < len(indexElems)-1 {
			endIndex, _ = strconv.Atoi(indexElems[i+1].Value)
		}
		dec := gob.NewDecoder(bytes.NewBuffer(d.buffer.Bytes()[startIndex:endIndex]))
		var pair Pair

		for dec.Decode(&pair) == nil {
			pairs = append(pairs, pair)
		}
	}

	return pairs
}

func (d *DiskBlock) Del(key string) error {
	if d.Empty() {
		return nil
	}

	start_, err := d.index.GreatestKeyLessThanOrEqualTo(key)

	if err != nil {
		return err
	}

	startIndex, _ := strconv.Atoi(start_.Value)

	end_, err := d.index.SmallestKeyGreaterThan(key)

	if err != nil {
		return err
	}

	endIndex, _ := strconv.Atoi(end_.Value)

	searchBuffer := bytes.NewBuffer(d.buffer.Bytes()[startIndex:endIndex])
	dec := gob.NewDecoder(searchBuffer)

	var pairs []Pair

	for {
		var pair Pair
		if err := dec.Decode(&pair); err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if pair.Key != key {
			pairs = append(pairs, pair)
		}
	}

	d.buffer.Reset()

	for _, pair := range pairs {
		encoder := gob.NewEncoder(&d.buffer)
		encoder.Encode(pair)
	}

	d.NumOfElements--
	return nil
}

func (d *DiskBlock) Empty() bool {
	return d.NumOfElements == 0
}
