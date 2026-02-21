package go_db

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1max <= BTREE_PAGE_SIZE)
}

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

type BNode []byte

type BTree struct {
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) []byte // deref a pointer
	new func([]byte) uint64 // allocate a new page
	del func(uint64)        // deallocate a page
}

// header functions
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(index uint16) uint64 {
	assert(index < node.nkeys())
	pos := HEADER + 8*index
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(index uint16, value uint64) {
	assert(index < node.nkeys())
	pos := HEADER + 8*index
	binary.LittleEndian.PutUint64(node[pos:], value)
}

// offset list
func offsetPos(node BNode, index uint16) uint16 {
	assert(index >= 1 && index <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(index-1)
}

func (node BNode) getOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, index):])
}

func (node BNode) setOffset(index uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, index):], offset)
}

// key-values
func (node BNode) kvPos(index uint16) uint16 {
	assert(index <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(index)
}

func (node BNode) getKey(index uint16) []byte {
	assert(index < node.nkeys())
	pos := node.kvPos(index)
	keyLen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:keyLen]
}

func (node BNode) getValue(index uint16) []byte {
	assert(index < node.nkeys())
	pos := node.kvPos(index)
	keyLen := binary.LittleEndian.Uint16(node[pos+0:])
	valueLen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+keyLen:][:valueLen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func assert(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}

// find last position <= key
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	var i uint16
	for i = 0; i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			return i
		}
		if cmp > 0 {
			return i - 1
		}
	}

	return i - 1
}

// add new key to leaf node
func leafInsert(new BNode, old BNode, index uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendKV(new, index, 0, key, val)
	nodeAppendRange(new, old, index+1, index, old.nkeys()-index)
}

/* update a leaf node
--create copy of leaf being operated on
--append range of node
--append KV into updated node
--append range of node after KV append
*/
func leafUpdate(
	new BNode, old BNode, index uint16, key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendKV(new, index, 0, key, val)
	nodeAppendRange(new, old, index+1, index+1, old.nkeys()-(index+1))
}

// replace a link with the same key
func nodeReplaceKid1ptr(new BNode, old BNode, index uint16, ptr uint64) {
	copy(new, old[:old.nbytes()])
	new.setPtr(index, ptr) // only the pointer is changed
}

// replace a link with multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, index uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	if inc == 1 && bytes.Equal(kids[0].getKey(0), old.getKey(index)) {
		// common case, only replace 1 pointer
		nodeReplaceKid1ptr(new, old, index, tree.new(kids[0]))
		return
	}

	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, index)
	for i, node := range kids {
		nodeAppendKV(new, index+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, index+inc, index+1, old.nkeys()-(index+1))
}

// replace 2 adjacent links with 1
func nodeReplace2Kid(
	new BNode, old BNode, index uint16,
	ptr uint64, key []byte,
) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendKV(new, index, ptr, key, nil)
	nodeAppendRange(new, old, index+1, index+2, old.nkeys()-(index+2))
}

// copy a KV into the position
/*
--set pointer to desired leaf
--assert position of KV
--place new KV in new node
--copy KV into desired position
--set the new offset after copy
*/
func nodeAppendKV(new BNode, index uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(index, ptr)
	// KVs
	pos := new.kvPos(index)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffset(index+1, new.getOffset(index)+4+uint16((len(key)+len(val))))
}

// copy multiple KVs into the position
/*
--assert that the desired source and location are not overflowed
--append KVs from source to destination in a loop
*/
func nodeAppendRange(
	new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16,
) {
	assert(srcOld+n <= old.nkeys() && dstNew+n <= new.nkeys())
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		nodeAppendKV(new, dst,
			old.getPtr(src), old.getKey(src), old.getValue(src))
	}
}

// Split an oversized node into 2 nodes. The 2nd node always fits.
func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2)
	// the initial guess
	nleft := old.nkeys() / 2
	// try to fit the left half
	left_bytes := func() uint16 {
		return HEADER + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	assert(nleft >= 1)
	// try to fit the right half
	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + HEADER
	}
	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	assert(nleft < old.nkeys())
	nright := old.nkeys() - nleft
	// new nodes
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	// NOTE: the left half may be still too big
	assert(right.nbytes() <= BTREE_PAGE_SIZE)
}

// split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 nodes
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}

// insert a KV into a node, the result might be split.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// The extra size allows it to exceed 1 page temporarily.
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	// where to insert the key?
	index := nodeLookupLE(node, key) // node.getKey(index) <= key
	switch node.btype() {
	case BNODE_LEAF: // leaf node
		if bytes.Equal(key, node.getKey(index)) {
			leafUpdate(new, node, index, key, val) // found, update it
		} else {
			leafInsert(new, node, index+1, key, val) // not found. insert
		}
	case BNODE_NODE: // internal node, walk into the child node
		nodeInsert(tree, new, node, index, key, val)
	default:
		panic("bad node!")
	}
	return new
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(
	tree *BTree, new BNode, node BNode, index uint16,
	key []byte, val []byte,
) {
	// recursive insertion to the kid node
	kptr := node.getPtr(index)
	knode := treeInsert(tree, tree.get(kptr), key, val)
	// after insertion, split the result
	nsplit, split := nodeSplit3(knode)
	// deallocate the old kid node
	tree.del(kptr)
	// update the kid links
	nodeReplaceKidN(tree, new, node, index, split[:nsplit]...)
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, index uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendRange(new, old, index, index+1, old.nkeys()-(index+1))
}

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
	assert(new.nbytes() <= BTREE_PAGE_SIZE)
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// where to find the key?
	index := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(index)) {
			return BNode{} // not found
		}
		// delete the key in the leaf
		new := BNode(make([]byte, BTREE_PAGE_SIZE))
		leafDelete(new, node, index)
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, index, key)
	default:
		panic("bad node!")
	}
}

// part of the treeDelete()
func nodeDelete(tree *BTree, node BNode, index uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(index)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr)

	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, index, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(index - 1))
		nodeReplace2Kid(new, node, index-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(index + 1))
		nodeReplace2Kid(new, node, index, tree.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && index == 0) // 1 empty child but no sibling
		new.setHeader(BNODE_NODE, 0)            // the parent becomes empty too
	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceKidN(tree, new, node, index, updated)
	}
	return new
}

// should the updated kid be merged with a sibling?
func shouldMerge(
	tree *BTree, node BNode,
	index uint16, updated BNode,
) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if index > 0 {
		sibling := BNode(tree.get(node.getPtr(index - 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}
	if index+1 < node.nkeys() {
		sibling := BNode(tree.get(node.getPtr(index + 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}
	return 0, BNode{}
}

func checkLimit(key []byte, val []byte) error {
	if len(key) == 0 {
		return errors.New("empty key") // used as a dummy key
	}
	if len(key) > BTREE_MAX_KEY_SIZE {
		return errors.New("key too long")
	}
	if len(val) > BTREE_MAX_VAL_SIZE {
		return errors.New("value too long")
	}
	return nil
}

// the interface
func (tree *BTree) Insert(key []byte, val []byte) error {
	if err := checkLimit(key, val); err != nil {
		return err // the only way for an update to fail
	}

	if tree.root == 0 {
		// create the first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return nil
	}

	node := treeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		// the root was split, add a new level.
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
	return nil
}

func (tree *BTree) Delete(key []byte) (bool, error) {
	if err := checkLimit(key, nil); err != nil {
		return false, err // the only way for an update to fail
	}

	if tree.root == 0 {
		return false, nil
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated) == 0 {
		return false, nil // not found
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		// remove a level
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true, nil
}

func nodeGetKey(tree *BTree, node BNode, key []byte) ([]byte, bool) {
	index := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(index)) {
			return node.getValue(index), true
		} else {
			return nil, false
		}
	case BNODE_NODE:
		return nodeGetKey(tree, tree.get(node.getPtr(index)), key)
	default:
		panic("bad node!")
	}
}

func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}
	return nodeGetKey(tree, tree.get(tree.root), key)
}

