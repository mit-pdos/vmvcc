package tuple

import (
	"sync"
	"go-mvcc/config"
)

type Version struct {
	begin	uint64
	end		uint64
	val		uint64
}

/**
 * `tidown`
 *		An TID specifying which txn owns the permission to update this tuple.
 * `tidrd`
 *		An TID specifying the last txn (in the sense of the largest TID, not
 *		actual physical time) that reads this tuple.
 * `tidwr`
 *		An TID specifying the last txn (in the sense of the largest TID, not
 *		actual physical time) that writes this tuple. `tidwr` should be the same
 *		as the begin timestamp of the latest version. For tuples without any
 *		version, this field is set to 0.
 *
 * Invariants:
 *		1. `tidown != 0` -> this tuple is read-only
 *		2. `vers` not empty -> exists a version whose `end` is
 *			config.TID_SENTINEL
 *		3. `len(vers) != 0` -> `tidwr == vers[len(vers) - 1].begin`
 * 		4. `len(vers) == 0` -> `tidwr == 0`
 */
type Tuple struct {
	latch	*sync.Mutex
	rcond	*sync.Cond
	tidown	uint64
	tidrd	uint64
	tidwr	uint64
	vers	[]Version
}

/**
 * TODO: Maybe start from the end (i.e., the newest version).
 */
func findRightVer(tid uint64, vers []Version) (Version, bool) {
	var ret Version
	var found bool = false
	for _, ver := range vers {
		if ver.begin <= tid && tid < ver.end {
			ret = ver
			found = true
		}
	}
	return ret, found
}

/**
 * Preconditions:
 *
 * Postconditions:
 * 1. On a successful return, the txn `tid` get the permission to update this
 * tuple (when we also acquire the latch of this tuple).
 */
func (tuple *Tuple) Own(tid uint64) bool {
	tuple.latch.Lock()

	/**
	 * Return an error if this txn tries to update a tuple that another txn with
	 * higher TID has already read from or written to, as it would fail to
	 * satisfy serializability.
	 */
	if tid < tuple.tidrd || tid < tuple.tidwr {
		tuple.latch.Unlock()
		return false
	}

	/* Return an error if the latest version is already onwed by another txn. */
	if tuple.tidown != 0 {
		tuple.latch.Unlock()
		return false
	}

	/* Acquire the permission to update this tuple (will do on commit). */
	tuple.tidown = tid

	tuple.latch.Unlock()
	return true
}

/**
 * Preconditions:
 * 1. The txn `tid` has the permission to update this tuple.
 */
func (tuple *Tuple) AppendVersion(tid uint64, val uint64) {
	tuple.latch.Lock()

	/**
	 * Modify the lifetime of the previous latest version to end at `tid` unless
	 * the tuple has never been written to (i.e., contains no versions).
	 */
	if len(tuple.vers) > 0 {
		idx := len(tuple.vers) - 1
		/**
		 * tuple.vers[idx].end = tid
		 * Goose error: [future]: reference to other types of expressions
		 */
		verPrevRef := &tuple.vers[idx]
		verPrevRef.end = tid
	}

	/* Allocate a new version. */
	verNext := Version{
		begin	: tid,
		end		: config.TID_SENTINEL,
		val		: val,
	}
	tuple.vers = append(tuple.vers, verNext)

	/* Release the permission to update this tuple. */
	tuple.tidown = 0

	/* Record the TID of the last writer. */
	tuple.tidwr = tid

	/* Wake up txns waiting on reading this tuple. */
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
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
func (tuple *Tuple) ReadVersion(tid uint64) (uint64, bool) {
	tuple.latch.Lock()

	/**
	 * The only case where a writer can block a reader is when the reader is
	 * trying to read the latest version (`tid >= tuple.tidwr`) AND the latest
	 * version may change in the future (`tuple.tidown != 0`).
	 */
	for tid >= tuple.tidwr && tuple.tidown != 0 {
		tuple.rcond.Wait()
	}

	/**
	 * We'll be able to do a case analysis here:
	 *		1. `tid < tuple.tidwr` means we can read previous versions.
	 *		2. `tuple.tidown == 0` means that follow-up txns trying to append a
	 *		new version will either fail (if the writer's `tid` is lower than
	 *		this `tid`), or succeed (if the writer's `tid` is greater than this
	 *		`tid`) but with the guarantee that the `end` timestamp of the new
	 *		version is going to be greater than `tid`, so this txn can safely
	 *		read the latest version at this point.
	 */

	/**
	 * Record the TID of the last reader. This is done at the start so that if
	 * there is no right version for txn `tid` to read, earlier txns (i.e.,
	 * those with smaller TID) won't be able to create one.
	 */
	if tuple.tidrd < tid {
		tuple.tidrd = tid
	}

	ver, found := findRightVer(tid, tuple.vers)

	/* Return an error when there is no right version for txn `tid`. */
	if !found {
		tuple.latch.Unlock()
		return 0, false
	}

	val := ver.val

	tuple.latch.Unlock()
	return val, true
}

/**
 * Remove all versions whose `end` timestamp is less than or equal to `tid`.
 * Preconditions:
 */
func (tuple *Tuple) RemoveVersions(tid uint64) {
	tuple.latch.Lock()

	var idx uint64 = 0
	for _, ver := range tuple.vers {
		/**
		 * TODO: Break early, as `tuple.vers` are sorted. Goose currently does
		 * not support break within non-unbounded loops.
		 */
		if ver.end <= tid {
			idx++
		}
	}
	/**
	 * `idx` points to the first usable version. A special case where `idx =
	 * len(tuple.vers)` removes all versions.
	 * Note that `s = s[len(s):]` is acceptable, which makes `s` a slice with
	 * zeroed len and cap.
	 */
	tuple.vers = tuple.vers[idx:]

	tuple.latch.Unlock()
}

func MkTuple() *Tuple {
	tuple := new(Tuple)
	tuple.latch = new(sync.Mutex)
	tuple.rcond = sync.NewCond(tuple.latch)
	tuple.tidown = 0
	tuple.tidrd = 0
	tuple.tidwr = 0
	tuple.vers = make([]Version, 0, 16)
	return tuple
}

