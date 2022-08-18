package tuple

import (
	"sync"
	"github.com/mit-pdos/go-mvcc/common"
)

/**
 * The lifetime of a version starts from `begin` of itself to the `begin` of
 * next version; it's a half-open interval (].
 */
type Version struct {
	begin	uint64
	deleted	bool
	val		uint64
}

/**
 * `owned`: A boolean flag indicating whether some txn owns this tuple..
 *
 * `tidlast`:
 * 	An TID specifying the last txn (in the sense of the largest TID, not actual
 * 	physical time) that reads (TID) or writes (TID + 1) this tuple.
 *
 * `vers`: Physical versions.
 */
type Tuple struct {
	latch   *sync.Mutex
	rcond   *sync.Cond
	owned   bool
	tidlast uint64
	vers    []Version
}

func findRightVer(tid uint64, vers []Version) Version {
	var ver Version
	length := uint64(len(vers))
	var idx uint64 = 0
	for {
		if idx >= length {
			break
		}
		// ver = vers[length - (1 + idx)]
		ver = vers[length - idx - 1]
		if tid > ver.begin {
			break
		}
		idx++
	}
	return ver
}

/**
 * Preconditions:
 *
 * Postconditions:
 * 1. On a successful return, the txn `tid` get the permission to update this
 * tuple (when we also acquire the latch of this tuple).
 */
func (tuple *Tuple) Own(tid uint64) uint64 {
	tuple.latch.Lock()

	/**
	 * Return an error if this txn tries to update a tuple that another txn with
	 * higher TID has already read from or written to, as it would fail to
	 * satisfy serializability.
	 */
	if tid < tuple.tidlast {
		tuple.latch.Unlock()
		return common.RET_UNSERIALIZABLE
	}

	/* Return an error if the latest version is already owned by another txn. */
	if tuple.owned {
		tuple.latch.Unlock()
		return common.RET_RETRY
	}

	/* Acquire the permission to update this tuple (will do on commit). */
	tuple.owned = true

	tuple.latch.Unlock()
	return common.RET_SUCCESS
}

func (tuple *Tuple) WriteLock() {
	tuple.latch.Lock()
}

func (tuple *Tuple) appendVersion(tid uint64, val uint64) {
	/* Create a new version and add it to the version chain. */
	verNew := Version{
		begin	: tid,
		val		: val,
		deleted	: false,
	}
	tuple.vers = append(tuple.vers, verNew)

	/* Release the permission to update this tuple. */
	tuple.owned = false

	tuple.tidlast = tid + 1
}

/**
 * Preconditions:
 * 1. The txn `tid` has the permission to update this tuple.
 */
func (tuple *Tuple) AppendVersion(tid uint64, val uint64) {
	tuple.appendVersion(tid, val)

	/* Wake up txns waiting on reading this tuple. */
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
}

func (tuple *Tuple) killVersion(tid uint64) bool {
	/**
	 * TODO: Check if the last version is already deleted; if so, simply return
	 * false.
	 */

	/* Create a tombstone and add it to the version chain. */
	verNew := Version{
		begin	: tid,
		deleted	: true,
	}
	tuple.vers = append(tuple.vers, verNew)

	/* Release the permission to update this tuple. */
	tuple.owned = false

	tuple.tidlast = tid + 1

	/* TODO: Differentiate a successful and a no-op deletion. */
	return true
}

/**
 * Preconditions:
 * 1. The txn `tid` has the permission to update this tuple.
 */
func (tuple *Tuple) KillVersion(tid uint64) uint64 {
	ok := tuple.killVersion(tid)
	var ret uint64
	if ok {
		ret = common.RET_SUCCESS
	} else {
		/* The tuple is already deleted. */
		ret = common.RET_NONEXIST
	}

	/* Wake up txns waiting on reading this tuple. */
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()

	return ret
}

/**
 * Preconditions:
 */
func (tuple *Tuple) Free(tid uint64) {
	tuple.latch.Lock()

	/* Release the permission to update this tuple. */
	tuple.owned = false

	/* Wake up txns waiting on reading this tuple. */
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
}

func (tuple *Tuple) ReadWait(tid uint64) {
	tuple.latch.Lock()

	/**
	 * The only case where a writer can block a reader is when the reader is
	 * trying to read the latest version (`tid > tuple.verlast.begin`) AND the
	 * latest version may change in the future (`tuple.owned`).
	 */
	for tid > tuple.tidlast && tuple.owned {
		/* TODO: Add timeout-retry to avoid deadlock. */
		tuple.rcond.Wait()
	}

	/**
	 * We'll be able to do a case analysis here:
	 * 1. `tid <= tuple.tidlast` means that the logical tuple has already been
	 * fixed at `tid`, so we can safety read it.
	 * 2. `tuple.owned` means that follow-up txns trying to append a new
	 * version will either fail (if the writer's `tid` is lower than this
	 * `tid`), or succeed (if the writer's `tid` is greater than this `tid`)
	 * but with the guarantee that the `end` timestamp of the new version is
	 * going to be greater than `tid`, so this txn can safely read the latest
	 * version at this point.
	 */
}

/**
 * Preconditions:
 */
func (tuple *Tuple) ReadVersion(tid uint64) (uint64, bool) {
	/**
	 * Try to find the right version from the version list.
	 */
	ver := findRightVer(tid, tuple.vers)

	/**
	 * Record the TID of the last reader/writer of this tuple.
	 */
	if tuple.tidlast < tid {
		tuple.tidlast = tid
	}

	tuple.latch.Unlock()
	return ver.val, !ver.deleted
}

func (tuple *Tuple) removeVersions(tid uint64) {
	/* `tuple.vers` is never empty. */
	var idx uint64
	idx = uint64(len(tuple.vers)) - 1
	for {
		if idx == 0 {
			break
		}
		ver := tuple.vers[idx]
		if ver.begin < tid {
			break
		}
		idx--
	}
	/**
	 * `idx` points to the first usable version.
	 */
	tuple.vers = tuple.vers[idx:]
}

/**
 * Remove all versions whose `end` timestamp is less than or equal to `tid`.
 * Preconditions:
 */
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
	tuple.tidlast = 1
	tuple.vers = make([]Version, 1, 16)
	tuple.vers[0] = Version{
		deleted : true,
	}
	/* Goose does not support: tuple.vers[0].deleted = true. */
	return tuple
}

