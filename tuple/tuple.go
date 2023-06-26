package tuple

import (
	"sync"
	"github.com/mit-pdos/go-mvcc/common"
)


// @ts
// Starting timestamp of this version, and also ending timestamp of the next
// version. Lifetime is a half-open interval: (ts of this, ts of next].
//
// @del
// Tombstone of this version.
//
// @val
// Value of this version.
type Version struct {
	ts  uint64
	del bool
	val string
}

// @owned
// Write lock of this tuple. Acquired before committing.
//
// @rcond
// Condition variable to wake up readers of this tuple.
//
// @tslast
// Timestamp of the last reader or last writer + 1.
//
// @vers
// Versions.
type Tuple struct {
	latch  *sync.Mutex
	rcond  *sync.Cond
	owned  bool
	tslast uint64
	vers   []Version
}

func findVersion(tid uint64, vers []Version) Version {
	var ver Version
	length := uint64(len(vers))
	var idx uint64 = 0
	for idx < length {
		ver = vers[length - idx - 1]
		if tid > ver.ts {
			break
		}
		idx++
	}
	return ver
}

func (tuple *Tuple) Own(tid uint64) uint64 {
	tuple.latch.Lock()

	// Return an error if the caller txn tries to update the tuple that has
	// already been read or written by another txn with a higher timestamp. This
	// ensures serializability.
	if tid < tuple.tslast {
		tuple.latch.Unlock()
		return common.RET_UNSERIALIZABLE
	}

	// Return an error if the latest version is already owned by another txn.
	if tuple.owned {
		tuple.latch.Unlock()
		return common.RET_RETRY
	}

	// Acquire the permission to update this tuple (will do on commit).
	tuple.owned = true

	tuple.latch.Unlock()
	return common.RET_SUCCESS
}

// Call @WriteOpen before @AppendVersion and @KillVersion.
func (tuple *Tuple) WriteOpen() {
	tuple.latch.Lock()
}

func (tuple *Tuple) appendVersion(tid uint64, val string) {
	// Create a new version and add it to the version chain.
	verNew := Version{
		ts  : tid,
		del : false,
		val : val,
	}
	tuple.vers = append(tuple.vers, verNew)

	// Release the permission to update this tuple.
	tuple.owned = false

	tuple.tslast = tid + 1
}

// Append a new version (@tid, @val) to this tuple.
func (tuple *Tuple) AppendVersion(tid uint64, val string) {
	tuple.appendVersion(tid, val)

	// Wake up txns waiting on reading this tuple.
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
}

func (tuple *Tuple) killVersion(tid uint64) bool {
	// Create a tombstone and add it to the version chain.
	verNew := Version{
		ts : tid,
		del : true,
	}
	tuple.vers = append(tuple.vers, verNew)

	// Release the permission to update this tuple.
	tuple.owned = false

	tuple.tslast = tid + 1

	// TODO: Differentiate a successful and a no-op deletion.
	return true
}

// Append a tombstone version of timestamp @tid to this tuple.
func (tuple *Tuple) KillVersion(tid uint64) uint64 {
	ok := tuple.killVersion(tid)
	var ret uint64
	if ok {
		ret = common.RET_SUCCESS
	} else {
		ret = common.RET_NONEXIST
	}

	// Wake up txns waiting on reading this tuple.
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
	return ret
}

// Release the write lock without modifying the tuple.
func (tuple *Tuple) Free() {
	tuple.latch.Lock()

	// Release the permission to update this tuple.
	tuple.owned = false

	// Wake up txns waiting on reading this tuple.
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
}

// Call @ReadWait before @ReadVersion.
// This design allows us to resolve the prophecy at the txn level.
func (tuple *Tuple) ReadWait(tid uint64) {
	tuple.latch.Lock()

	
	// The only case where a writer would block a reader is when the reader is
	// trying to read the latest version (i.e., @tid > @tuple.ts) AND the latest
	// version may change in the future (i.e., @tuple.owned = true).
	for tid > tuple.tslast && tuple.owned {
		tuple.rcond.Wait()
	}

	// After the loop, we'll be able to do a case analysis here:
	//
	// Case 1: [@tid <= @tuple.tslast]
	// The linear view has been fixed up to @tid, so we can safely read it.
	//
	// Case 2: [@tuple.owned = false]
	// Follow-up txns trying to modify this tuple will either fail (if the
	// writer's timestamp is smaller than @tid), or succeed (if the writer's
	// timestamp is greater than @tid) but with the guarantee that the new
	// version is going to be end *after* @tid, so we can safely read the latest
	// version at this point.
}

func (tuple *Tuple) ReadVersion(tid uint64) (string, bool) {
	ver := findVersion(tid, tuple.vers)

	if tuple.tslast < tid {
		// Record the timestamp of the last reader.
		tuple.tslast = tid
	}

	tuple.latch.Unlock()
	return ver.val, !ver.del
}

func (tuple *Tuple) removeVersions(tid uint64) {
	// Require @tuple.vers is not empty.
	var idx uint64
	idx = uint64(len(tuple.vers)) - 1
	for idx != 0 {
		ver := tuple.vers[idx]
		if ver.ts < tid {
			break
		}
		idx--
	}
	
	// Ensure @idx points to the first usable version.
	tuple.vers = tuple.vers[idx:]
}

// Remove all versions whose lifetime ends before @tid.
func (tuple *Tuple) RemoveVersions(tid uint64) {
	tuple.latch.Lock()
	tuple.removeVersions(tid)
	tuple.latch.Unlock()
}

func MkTuple() *Tuple {
	tuple := new(Tuple)
	tuple.latch = new(sync.Mutex)
	tuple.rcond = sync.NewCond(tuple.latch)
	tuple.owned = false
	tuple.tslast = 1
	tuple.vers = make([]Version, 1, 1)
	// Not supported by Goose:
	// tuple.vers[0].deleted = true
	tuple.vers[0] = Version{
		del: true,
	}
	return tuple
}
