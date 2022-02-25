package tuple

import (
	"sync"
	"github.com/mit-pdos/go-mvcc/config"
	"github.com/mit-pdos/go-mvcc/common"
)

/**
 * The lifetime of a version is a half-open interval `(begin, val]`.
 */
type Version struct {
	begin	uint64
	end		uint64
	val		uint64
}

/**
 * `tidown`
 *		An TID specifying which txn owns the permission to write this tuple.
 * `tidlast`
 *		An TID specifying the last txn (in the sense of the largest TID, not
 *		actual physical time) that reads or writes this tuple.
 */
type Tuple struct {
	latch	*sync.Mutex
	rcond	*sync.Cond
	tidown	uint64
	tidlast	uint64
	verlast	Version
	vers	[]Version
}

/**
 * TODO: Maybe start from the end (i.e., the newest version).
 * TODO: Can simply return a value rather than a version.
 */
func findRightVer(tid uint64, vers []Version) (Version, uint64) {
	var ver Version
	var ret uint64 = common.RET_NONEXIST
	for _, v := range vers {
		if v.begin < tid && tid <= v.end {
			ver = v
			ret = common.RET_SUCCESS
		}
	}
	return ver, ret
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
	if tuple.tidown != 0 {
		tuple.latch.Unlock()
		return common.RET_RETRY
	}

	/* Acquire the permission to update this tuple (will do on commit). */
	tuple.tidown = tid

	tuple.latch.Unlock()
	return common.RET_SUCCESS
}

func (tuple *Tuple) appendVersion(tid uint64, val uint64) {
	/**
	 * Modify the lifetime of the last version if it has not been deleted.
	 */
	var verLast Version
	verLast = tuple.verlast
	if verLast.end == config.TID_SENTINEL {
		verLast.end = tid
	}
	tuple.vers = append(tuple.vers, verLast)

	/* Create a new version. */
	verNext := Version{
		begin	: tid,
		end		: config.TID_SENTINEL,
		val		: val,
	}
	tuple.verlast = verNext

	/* Release the permission to update this tuple. */
	tuple.tidown = 0

	tuple.tidlast = tid
}

/**
 * Preconditions:
 * 1. The txn `tid` has the permission to update this tuple.
 */
func (tuple *Tuple) AppendVersion(tid uint64, val uint64) {
	tuple.latch.Lock()

	tuple.appendVersion(tid, val)

	/* Wake up txns waiting on reading this tuple. */
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
}

func (tuple *Tuple) killVersion(tid uint64) uint64 {
	var ret uint64

	if tuple.verlast.end == config.TID_SENTINEL {
		ret = common.RET_SUCCESS
	} else {
		ret = common.RET_NONEXIST
	}
	tuple.verlast.end = tid

	/* Release the permission to update this tuple. */
	tuple.tidown = 0

	tuple.tidlast = tid

	return ret
}

/**
 * Preconditions:
 * 1. The txn `tid` has the permission to update this tuple.
 */
func (tuple *Tuple) KillVersion(tid uint64) uint64 {
	tuple.latch.Lock()

	ret := tuple.killVersion(tid)

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
	tuple.tidown = 0

	/* Wake up txns waiting on reading this tuple. */
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
}

/**
 * Preconditions:
 */
func (tuple *Tuple) ReadVersion(tid uint64) (uint64, uint64) {
	tuple.latch.Lock()

	/**
	 * The only case where a writer can block a reader is when the reader is
	 * trying to read the latest version (`tid > tuple.verlast.begin`) AND the
	 * latest version may change in the future (`tuple.tidown != 0`).
	 *
	 * TODO: An optimization we can do here is checking `tid < tidlast`.
	 * Not sure if it's effective though.
	 */
	var verLast Version
	verLast = tuple.verlast
	for tid > verLast.begin && tuple.tidown != 0 {
		/* TODO: Add timeout-retry to avoid deadlock. */
		tuple.rcond.Wait()
		verLast = tuple.verlast
	}

	/**
	 * We'll be able to do a case analysis here:
	 * 1. `tid <= tuple.verlast.begin` means we can read previous versions.
	 * 2. `tuple.tidown == 0` means that follow-up txns trying to append a new
	 *    version will either fail (if the writer's `tid` is lower than this
	 *    `tid`), or succeed (if the writer's `tid` is greater than this `tid`)
	 *    but with the guarantee that the `end` timestamp of the new version is
	 *    going to be greater than `tid`, so this txn can safely read the
	 *    latest version at this point.
	 */

	/**
	 * Try to find the right version from the list of previous versions if
	 * `tid` is less than or equal to the begin timestamp of the last version.
	 */
	if tid <= verLast.begin {
		ver, found := findRightVer(tid, tuple.vers)
		tuple.latch.Unlock()
		return ver.val, found
	}

	/**
	 * Check whether `tid` lies within the lifetime of the last version.
	 */
	var val uint64
	var ret uint64
	if tid <= verLast.end {
		val = verLast.val
		ret = common.RET_SUCCESS
	} else {
		ret = common.RET_NONEXIST
	}

	/**
	 * Record the TID of the last reader/writer of this tuple.
	 */
	if tuple.tidlast < tid {
		tuple.tidlast = tid
	}

	tuple.latch.Unlock()
	return val, ret
}

func (tuple *Tuple) removeVersions(tid uint64) {
	var idx uint64 = 0
	for {
		if idx >= uint64(len(tuple.vers)) {
			break
		}
		ver := tuple.vers[idx]
		if ver.end > tid {
			break
		}
		idx++
	}
	/**
	 * `idx` points to the first usable version. A special case where `idx =
	 * len(tuple.vers)` removes all versions.
	 * Note that `s = s[len(s):]` is acceptable, which makes `s` a slice with
	 * zeroed len and cap.
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
	tuple.tidown = 0
	tuple.tidlast = 0
	tuple.verlast = Version{
		begin : 0,
		end   : 0,
		val   : 0,
	}
	tuple.vers = make([]Version, 0, 16)
	return tuple
}

