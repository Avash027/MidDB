package LsmTree

import "fmt"

type TreeNode struct {
	Size  int
	Left  *TreeNode
	Right *TreeNode
	Data  Pair
}

func NewTreeNode(elements []Pair) *TreeNode {
	// create a tree using recursion
	if len(elements) == 0 {
		return &TreeNode{}
	}

	if len(elements) == 1 {
		return &TreeNode{Data: elements[0]}
	}

	mid := len(elements) / 2

	return &TreeNode{
		Size:  len(elements),
		Left:  NewTreeNode(elements[:mid]),
		Right: NewTreeNode(elements[mid+1:]),
		Data:  elements[mid],
	}
}

func (tree *TreeNode) Find(key string) (Pair, error) {
	if tree == nil {
		return Pair{}, fmt.Errorf("key not found")
	}

	if tree.Data.Key == key {
		return tree.Data, nil
	}

	if tree.Data.Key >= key {
		return tree.Left.Find(key)
	}

	return tree.Right.Find(key)

}

func Delete(tree **TreeNode, key string) {
	if *tree == nil {
		return
	} else if key < (*tree).Data.Key {
		Delete(&((*tree).Left), key)
		(*tree).Size++
	} else if key > (*tree).Data.Key {
		Delete(&((*tree).Right), key)
		(*tree).Size++
	} else {
		(*tree).Data.Tombstone = true
	}

}

func Insert(tree **TreeNode, pair Pair) {
	if *tree == nil {
		*tree = &TreeNode{Data: pair, Size: 1}
	} else if pair.Key < (*tree).Data.Key {
		Insert(&((*tree).Left), pair)
		(*tree).Size++
	} else if pair.Key > (*tree).Data.Key {
		Insert(&((*tree).Right), pair)
		(*tree).Size++
	} else {
		(*tree).Data.Value = pair.Value
		(*tree).Data.Tombstone = false
	}

}

func (tree *TreeNode) GetSize() int {
	return tree.Size
}

func (tree *TreeNode) All() []Pair {
	if tree == nil {
		return []Pair{}
	}

	return append(append(tree.Left.All(), tree.Data), tree.Right.All()...)
}

func (tree *TreeNode) GreatestKeyLessThanOrEqualTo(key string) (Pair, error) {
	if tree == nil {
		return Pair{}, fmt.Errorf("key %s is smaller than all keys in the tree", key)
	}

	currentData := tree.Data

	if currentData.Key == key {
		return currentData, nil
	}

	if currentData.Key <= key {
		rightSubTree, err := tree.Right.GreatestKeyLessThanOrEqualTo(key)
		if err == nil && rightSubTree.Key > currentData.Key {
			currentData = rightSubTree
		}
	} else {
		leftSubTree, err := tree.Left.GreatestKeyLessThanOrEqualTo(key)
		if err != nil {
			return Pair{}, err
		}
		currentData = leftSubTree
	}

	return currentData, nil

}

func (tree *TreeNode) SmallestKeyGreaterThan(key string) (Pair, error) {
	if tree == nil {
		return Pair{}, fmt.Errorf("key not found")
	}

	currentData := tree.Data

	if currentData.Key > key {
		leftSubTree, err := tree.Left.SmallestKeyGreaterThan(key)
		if err == nil && currentData.Key > leftSubTree.Key {
			currentData = leftSubTree
		}
	} else {
		rightSubTree, err := tree.Right.SmallestKeyGreaterThan(key)
		if err != nil {
			return Pair{}, err
		}
		currentData = rightSubTree
	}
	return currentData, nil
}
