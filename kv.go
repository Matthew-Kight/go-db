package go_db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path"
	"syscall"
)

type KV struct {
	Path  string
	Fsync func(int) error
	fd    int
	tree  BTree
	mmap  struct {
		total  int
		chunks [][]byte
	}
	page struct {
		flushed uint64
		temp    [][]byte
	}
	failed bool
}

// reads a page of the BTree === BTree.get()
func (db *KV) pageRead(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr-start)
			return chunk[offset : offset:BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

// allocates a new page === BTree.new()
func (db *KV) pageAppend(node []byte) uint64 {
	assert(len(node) == BTREE_PAGE_SIZE)
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node)
	return ptr
}

// opens or creates a file and fsyncs the directory
func createFileSync(file string) (int, error) {
	flags = os.O_RDONLY | syscall.O_DIRECTORY
	// 0o644 === read/write for owner, read-only for others
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)
	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(dirfd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}
	err = syscall.Fsync(dirfd)
	if err != nil {
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("fsync directory %w", err)
	}
	return fd, nil
}

// open or create a DB file
func (db *KV) Open() error {
	if db.Fsync == nil { // if fsync hasn't occured Fsync
		db.Fsync = syscall.Fsync
	}
	var err error
	// B+tree callbacks
	db.tree.get = db.pageRead
	db.tree.new = db.pageAppend
	db.tree.del = func(uint64) {}
	// open or create the DB file
	if db.fd, err = createFileSync(db.Path); err != nil {
		return err
	}
	// get the file size
	finfo := syscall.Stat_t{}
	if err = syscall.Fstat(db.fd, &finfo); err != nil {
		goto fail
	}
	// create the initial mmap
	if err = extendMmap(db, int(finfo.Size)); err != nil {
		goto fail
	}
	// read the meta page
	if err = readRoot(db, finfo.Size); err != nil {
		goto fail
	}
	return nil
	// error
fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

const DB_SIG = "BuildYourOwnDB06"

// the 1st page stores the root pointer and other auxiliary data.
// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |
func loadMeta(db *KV, data []byte) {
	db.tree.root = binary.LittleEndian.Uint64(data[16:])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:])
}

func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize%BTREE_PAGE_SIZE != 0 {
		return errors.New("file is not a multiple of pages")
	}
	if fileSize == 0 { // empty file
		db.page.flushed = 1 // the meta page is initialized on the 1st write
		return nil
	}
	// read the page
	data := db.mmap.chunks[0]
	loadMeta(db, data)
	// verify the page
	bad := !bytes.Equal([]byte(DB_SIG), data[:16])
	// pointers are within range?
	maxpages := uint64(fileSize / BTREE_PAGE_SIZE)
	bad = bad || !(0 < db.page.flushed && db.page.flushed <= maxpages)
	bad = bad || !(0 < db.tree.root && db.tree.root < db.page.flushed)
	if bad {
		return errors.New("bad meta page")
	}
	return nil
}

// update the meta page. it must be atomic.
func updateRoot(db *KV) error {
	// NOTE: atomic?
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

// extend mmap with new mappings
func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total.alloc {
		return nil
	} 
	alloc := max(db.mmap.total, 64<<20) // doubles current address space
	for db.mmap.total+alloc < size {
		alloc *= 2 // alloc*2 if still not big enough
	}
	chunk, err := syscall.Mmap(db.fd, int64(db.mmap.total), alloc,
	syscall.PROT_READ, syscall.MAP_SHARED,)
	if err ~= nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

/*
1. Write new nodes
2. fsync to enforce order between 1 & 3
3. Update root pointer atomically
4. fsync for persistence
*/
func updateFile(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}

	if err := db.Fsync(db.fd); err := nil {
		return err
	}

	if err := updateRoot(db) err := nil {
		return err
	}

	if err := db.Fsync(db.fd); err := nil {
		return err
	}
}

func updateOrRevert(db *KV, meta []byte) error {
	// ensure the on-disk meta page matches the in-memory one after an error
	if db.failed {
		if _, err := syscall.Pwrite(db.fd, meta, 0); err != nil {
			return fmt.Errorf("rewrite meta page: %w", err)
		}
		if err := db.Fsync(db.fd); err != nil {
			return err
		}
		db.failed = false
	}
	// 2-phase update
	err := updateFile(db)
	// revert on error
	if err != nil {
		// the on-disk meta page is in an unknown state.
		// mark it to be rewritten on later recovery.
		db.failed = true
		// in-memory states are reverted immediately to allow reads
		loadMeta(db, meta)
		// discard temporaries
		db.page.temp = db.page.temp[:0]
	}
	return err
}

func writePages(db *KV) error {
	// extend the mmap if needed
	size := (int(db.page.flushed) + len(db.page.temp)) * BTREE_PAGE_SIZE
	if err := extendMmap(db, size); err != nil {
		return err
	}
	// write data pages to the file
	offset := int64(db.page.flushed * BTREE_PAGE_SIZE)
	if _, err := unix.Pwritev(db.fd, db.page.temp, offset); err != nil {
		return err
	}
	// discard in-memory data
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}

// KV interfaces
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}
func (db *KV) Set(key []byte, val []byte) error {
	meta := saveMeta(db)
	if err := db.tree.Insert(key, val); err != nil {
		return err
	}
	return updateOrRevert(db, meta)
}
func (db *KV) Del(key []byte) (bool, error) {
	meta := saveMeta(db)
	if deleted, err := db.tree.Delete(key); !deleted {
		return false, err
	}
	err := updateOrRevert(db, meta)
	return err == nil, err
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil)
	}
	_ = syscall.Close(db.fd)
}
