package tuple

import (
	"sync"
)

const (
	MAX_U64 uint64 = 18446744073709551615
)

type Version struct {
	begin	uint64
	end		uint64
	val		uint64
}

/**
 * `tidwr`
 *		An TID specifying which txn owns the permission to update this tuple.
 * `tidlast`
 *		An TID specifying the last txn (in the sense of the largest TID, not
 *		actual physical time) that reads or writes this tuple.
 *
 * Invariants:
 * 1. `tidwr != 0` -> this tuple is read-only
 * 2. `vers` not empty -> exists a version whose `end` is MAX_U64
 */
type Tuple struct {
	latch	*sync.Mutex
	rcond	*sync.Cond
	tidwr	uint64
	tidlast	uint64
	vers	[]Version
}

/**
 * TODO: Maybe start from the end (i.e., the newest version).
 */
func findRightVer(tid uint64, vers []Version) *Version {
	for _, ver := range vers {
		if ver.begin <= tid && tid < ver.end {
			return &ver
		}
	}
	return nil
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
	if tid < tuple.tidlast {
		tuple.latch.Unlock()
		return false
	}

	/* Return an error if the latest version is already write-locked. */
	if tuple.tidwr != 0 {
		tuple.latch.Unlock()
		return false
	}

	/* Acquire the permission to update this tuple (will do on commit). */
	tuple.tidwr = tid

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
	if len(tuple.vers) != 0 {
		idx := len(tuple.vers) - 1
		verPrev := tuple.vers[idx]
		verPrev.end = tid
	}

	/* Allocate a new version. */
	verNext := Version{
		begin	: tid,
		end		: MAX_U64,
		val		: val,
	}
	tuple.vers = append(tuple.vers, verNext)

	/* Release the permission to update this tuple. */
	tuple.tidwr = 0

	/* Record the TID of the last writer. */
	tuple.tidlast = tid

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
	tuple.tidwr = 0

	/* Wake up txns waiting on reading this tuple. */
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
}

/**
 * Preconditions:
 */
func (tuple *Tuple) ReadVersion(tid uint64) (uint64, bool) {
	tuple.latch.Lock()

	/* XXX: potential bug here */
	/**
	 * Record the TID of the last reader. This is done at the start so that if
	 * there is no right version for txn `tid` to read, earlier txn (i.e., those
	 * with smaller TID) won't be able to create one.
	 */
	if tuple.tidlast < tid {
		tuple.tidlast = tid
	}

	var ver *Version
	ver = findRightVer(tid, tuple.vers)

	/* Return an error when there is no right version for txn `tid`. */
	if ver == nil {
		return 0, false
	}

	/**
	 * The only case where a writer can block a reader is when the reader is
	 * trying to read the latest version (`ver.end == MAX_U64`) AND the latest
	 * version may change in the future (`tuple.tidwr != 0`).
	 */
	for ver.end == MAX_U64 && tuple.tidwr != 0 {
		tuple.rcond.Wait()

		/**
		 * Find the right version again as the writing txn can be either before
		 * or after this txn.
		 */
		ver = findRightVer(tid, tuple.vers)
	}

	val := ver.val

	tuple.latch.Unlock()
	return val, true
}

func MkTuple() *Tuple {
	tuple := new(Tuple)
	tuple.latch = new(sync.Mutex)
	tuple.rcond = sync.NewCond(tuple.latch)
	tuple.tidwr = 0
	tuple.tidlast = 0
	tuple.vers = make([]Version, 0)
	return tuple
}

