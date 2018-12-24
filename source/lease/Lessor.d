module lease.Lessor;

import std.typecons;
import std.math;
import core.sync.mutex;
import core.sync.rwmutex;
import std.bitmanip;

import hunt.time;
import hunt.logging;
import hunt.lang.exception;

import etcdserverpb.rpc;

import lease.Heap;
import lease.LeaseQueue;

alias int64 = long;
alias LeaseID = long;
alias float64 = long;

// NoLease is a special LeaseID representing the absence of a lease.
const LeaseID NoLease = LeaseID(0);

// MaxLeaseTTL is the maximum lease TTL value
const long MaxLeaseTTL = 9000000000;

enum long FOREVER = long.max;

enum string leaseBucketName = "lease";

// maximum number of leases to revoke per second; configurable for tests
enum int leaseRevokeRate = 1000;

// maximum number of lease checkpoints recorded to the consensus log per second; configurable for tests
enum int leaseCheckpointRate = 1000;

// maximum number of lease checkpoints to batch into a single consensus log entry
enum int maxLeaseCheckpointBatchSize = 1000;

enum string ErrNotPrimary = "not a primary lessor";
enum string ErrLeaseNotFound = "lease not found";
enum string ErrLeaseExists = "lease already exists";
enum string ErrLeaseTTLTooLarge = "too large lease TTL";

// TxnDelete is a TxnWrite that only permits deletes. Defined here
// to avoid circular dependency with mvcc.
interface TxnDelete
{
	Tuple!(int64, int64) DeleteRange(byte[] key, byte[] end);
	void End();
}

// RangeDeleter is a TxnDelete constructor.
alias RangeDeleter = TxnDelete delegate();
// type RangeDeleter func() TxnDelete

// Checkpointer permits checkpointing of lease remaining TTLs to the consensus log. Defined here to
// avoid circular dependency with mvcc.
alias Checkpointer = void delegate( /* context.Context ctx, */ LeaseCheckpointRequest lc);
// type Checkpointer func(ctx , lc *)

// Lessor owns leases. It can grant, revoke, renew and modify leases for lessee.
// type Lessor interface {
// 	// SetRangeDeleter lets the lessor create TxnDeletes to the store.
// 	// Lessor deletes the items in the revoked or expired lease by creating
// 	// new TxnDeletes.
// 	SetRangeDeleter(rd RangeDeleter)

// 	SetCheckpointer(cp Checkpointer)

// 	// Grant grants a lease that expires at least after TTL seconds.
// 	Grant(id LeaseID, ttl int64) (*Lease, error)
// 	// Revoke revokes a lease with given ID. The item attached to the
// 	// given lease will be removed. If the ID does not exist, an error
// 	// will be returned.
// 	Revoke(id LeaseID) error

// 	// Checkpoint applies the remainingTTL of a lease. The remainingTTL is used in Promote to set
// 	// the expiry of leases to less than the full TTL when possible.
// 	Checkpoint(id LeaseID, remainingTTL int64) error

// 	// Attach attaches given leaseItem to the lease with given LeaseID.
// 	// If the lease does not exist, an error will be returned.
// 	Attach(id LeaseID, items []LeaseItem) error

// 	// GetLease returns LeaseID for given item.
// 	// If no lease found, NoLease value will be returned.
// 	GetLease(item LeaseItem) LeaseID

// 	// Detach detaches given leaseItem from the lease with given LeaseID.
// 	// If the lease does not exist, an error will be returned.
// 	Detach(id LeaseID, items []LeaseItem) error

// 	// Promote promotes the lessor to be the primary lessor. Primary lessor manages
// 	// the expiration and renew of leases.
// 	// Newly promoted lessor renew the TTL of all lease to extend + previous TTL.
// 	Promote(extend time.Duration)

// 	// Demote demotes the lessor from being the primary lessor.
// 	Demote()

// 	// Renew renews a lease with given ID. It returns the renewed TTL. If the ID does not exist,
// 	// an error will be returned.
// 	Renew(id LeaseID) (int64, error)

// 	// Lookup gives the lease at a given lease id, if any
// 	Lookup(id LeaseID) *Lease

// 	// Leases lists all leases.
// 	Leases() []*Lease

// 	// ExpiredLeasesC returns a chan that is used to receive expired leases.
// 	ExpiredLeasesC() <-chan []*Lease

// 	// Recover recovers the lessor state from the given backend and RangeDeleter.
// 	Recover(b backend.Backend, rd RangeDeleter)

// 	// Stop stops the lessor for managing leases. The behavior of calling Stop multiple
// 	// times is undefined.
// 	Stop()
// }

// lessor implements Lessor interface.
// TODO: use clockwork for testability.
class Lessor
{
	// mu sync.RWMutex
	Mutex _mutex;
	// demotec is set when the lessor is the primary.
	// demotec will be closed if the lessor is demoted.
	// demotec chan struct{}

	Lease[LeaseID] leaseMap;
	Heap!LeaseQueue leaseHeap;
	Heap!LeaseQueue leaseCheckpointHeap;
	LeaseID[LeaseItem] itemMap;

	// When a lease expires, the lessor will delete the
	// leased range (or key) by the RangeDeleter.
	RangeDeleter rd;

	// When a lease's deadline should be persisted to preserve the remaining TTL across leader
	// elections and restarts, the lessor will checkpoint the lease by the Checkpointer.
	Checkpointer cp;

	// backend to persist leases. We only persist lease ID and expiry for now.
	// The leased items can be recovered by iterating all the keys in kv.
	// backend.Backend b;

	// minLeaseTTL is the minimum lease TTL that can be granted for a lease. Any
	// requests for shorter TTLs are extended to the minimum TTL.
	int64 minLeaseTTL;

	// expiredC chan []*Lease
	// stopC is a channel whose closure indicates that the lessor should be stopped.
	// stopC chan struct{}
	// doneC is a channel whose closure indicates that the lessor is stopped.
	// doneC chan struct{}

	// lg *zap.Logger

	// Wait duration between lease checkpoints.
	long checkpointInterval;

	this(int64 ttl, long interval)
	{
		minLeaseTTL = ttl;
		checkpointInterval = interval;
	}

	// isPrimary indicates if this lessor is the primary lessor. The primary
	// lessor manages lease expiration and renew.
	//
	// in etcd, raft leader is the primary. Thus there might be two primary
	// leaders at the same time (raft allows concurrent leader but with different term)
	// for at most a leader election timeout.
	// The old primary leader cannot affect the correctness since its proposal has a
	// smaller term and will not be committed.
	//
	// TODO: raft follower do not forward lease management proposals. There might be a
	// very small window (within second normally which depends on go scheduling) that
	// a raft follow is the primary between the raft leader demotion and lessor demotion.
	// Usually this should not be a problem. Lease should not be that sensitive to timing.

	bool isPrimary()
	{
		implementationMissing();
		return false;
		// return this.demotec != null;
	}

	void SetRangeDeleter(RangeDeleter rd)
	{
		_mutex.lock();
		scope (exit)
			_mutex.unlock();

		this.rd = rd;
	}

	void SetCheckpointer(Checkpointer cp)
	{
		_mutex.lock();
		scope (exit)
			_mutex.unlock();

		this.cp = cp;
	}

	Lease Grant(LeaseID id, int64 ttl)
	{
		if (id == NoLease)
		{
			return null;
		}

		if (ttl > MaxLeaseTTL)
		{
			return null;
		}

		// TODO: when lessor is under high load, it should give out lease
		// with longer TTL to reduce renew load.
		Lease l = new Lease();
		l.ID = id;
		l.ttl = ttl; // itemSet: make(map[LeaseItem]struct{}),
		// revokec: make(chan struct{}),

		_mutex.lock();
		scope (exit)
			_mutex.unlock();

		auto ok = this.leaseMap[id];
		if (ok)
		{
			// throw new Exception("ErrLeaseExists");
			return null;
		}

		if (l.ttl < this.minLeaseTTL)
		{
			l.ttl = this.minLeaseTTL;
		}

		if (this.isPrimary())
		{
			l.refresh(0);
		}
		else
		{
			l.forever();
		}

		this.leaseMap[id] = l;
		LeaseWithTime item = {id:
		l.ID, time : l.expiry /* .UnixNano() */ };
	this.leaseHeap.Push(item);

	l.persistTo( /* this.b */ );

	///@gxc
	// leaseTotalTTLs.Observe(float64(l.ttl));
	// leaseGranted.Inc();

	if (this.isPrimary())
	{
		this.scheduleCheckpointIfNeeded(l);
	}

	return l;
}

string Revoke(LeaseID id)
{
	_mutex.lock();

	auto l = this.leaseMap[id];
	if (l is null)
	{
		this._mutex.unlock();
		return ErrLeaseNotFound;
	}
	// scope (exit)
	// 	close(l.revokec);
	// unlock before doing external work
	this._mutex.unlock();

	if (this.rd is null)
	{
		return null;
	}

	auto txn = this.rd();

	// sort keys so deletes are in same order among all members,
	// otherwise the backened hashes will be different
	auto keys = l.Keys();
	// sort.StringSlice(keys).Sort();
	foreach (key; keys)
	{
		txn.DeleteRange(cast(byte[])(key), null);
	}

	_mutex.lock();
	scope (exit)
		_mutex.unlock();
	// delete(this.leaseMap, l.ID);
	this.leaseMap.remove(l.ID);
	// lease deletion needs to be in the same backend transaction with the
	// kv deletion. Or we might end up with not executing the revoke or not
	// deleting the keys if etcdserver fails in between.
	// this.b.BatchTx().UnsafeDelete(leaseBucketName, int64ToBytes(int64(l.ID)));

	txn.End();

	// leaseRevoked.Inc();
	return null;
}

string Checkpoint(LeaseID id, int64 remainingTTL)
{
	_mutex.lock();
	scope (exit)
		_mutex.unlock();

	auto l = this.leaseMap[id];
	if (l !is null)
	{
		// when checkpointing, we only update the remainingTTL, Promote is responsible for applying this to lease expiry
		l.remainingTTL = remainingTTL;
		if (this.isPrimary())
		{
			// schedule the next checkpoint as needed
			this.scheduleCheckpointIfNeeded(l);
		}
	}
	return null;
}

// Renew renews an existing lease. If the given lease does not exist or
// has expired, an error will be returned.
int64 Renew(LeaseID id)
{
	_mutex.lock();

	scope (exit)
		_mutex.unlock();

	if (!this.isPrimary())
	{
		// forward renew request to primary instead of returning error.
		return -1;
	}

	// auto demotec = this.demotec;

	auto l = this.leaseMap[id];
	if (l is null)
	{
		return -1; /* , ErrLeaseNotFound */
	}

	if (l.expired())
	{
		this._mutex.unlock();
		// unlock = func() {}
		///@gxc
		// select
		// {
		// 	// A expired lease might be pending for revoking or going through
		// 	// quorum to be revoked. To be accurate, renew request must wait for the
		// 	// deletion to complete.
		// 	case  <  - l.revokec : return  - 1, ErrLeaseNotFound // The expired lease might fail to be revoked if the primary changes.
		// 	// The caller will retry on ErrNotPrimary.
		// 	case  <  - demotec
		// 		: return  - 1, ErrNotPrimarycase <  - this.stopC : return  - 1, ErrNotPrimary
		// }
		return -1;
	}

	// Clear remaining TTL when we renew if it is set
	// By applying a RAFT entry only when the remainingTTL is already set, we limit the number
	// of RAFT entries written per lease to a max of 2 per checkpoint interval.
	if (this.cp != null && l.remainingTTL > 0)
	{
		LeaseCheckpoint[] cps;
		LeaseCheckpoint ckp = new LeaseCheckpoint();
		ckp.ID = int64(l.ID);
		ckp.remainingTTL = 0;

		cps ~= ckp;
		LeaseCheckpointRequest req = new LeaseCheckpointRequest();
		req.checkpoints ~= ckp;
		this.cp( /* context.Background(),  */ req);
	}

	l.refresh(0);
	LeaseWithTime item = {id:
	l.ID, time : l.expiry /* .UnixNano() */ };
this.leaseHeap.Push(item);

// leaseRenewed.Inc();
return l.ttl /* , null */ ;
}

Lease Lookup(LeaseID id)
{
	this._mutex.lock();
	scope (exit)
		this._mutex.unlock();
	return this.leaseMap[id];
}

Lease[] unsafeLeases()
{
	Lease[] leases;
	foreach (LeaseID k, Lease v; this.leaseMap)
	{
		leases ~= v;
	}
	return leases;
}

Lease[] Leases()
{
	this._mutex.lock();
	auto ls = this.unsafeLeases();
	this._mutex.unlock();
	// sort.Sort(leasesByExpiry(ls));
	return ls;
}

void Promote(long extend)
{
	_mutex.lock();
	scope (exit)
		_mutex.unlock();

	// this.demotec = make(chan struct
	// 	{
	// });

	// refresh the expiries of all leases.
	foreach (LeaseID k, Lease l; this.leaseMap)
	{
		l.refresh(extend);
		LeaseWithTime item = {id:
		l.ID, time : l.expiry /* .UnixNano() */ };
	this.leaseHeap.Push(item);
}

if ((this.leaseMap.length) < leaseRevokeRate)
{
	// no possibility of lease pile-up
	return;
}

// adjust expiries in case of overlap
auto leases = this.unsafeLeases();
// sort.Sort(leasesByExpiry(leases));

auto baseWindow = leases[0].Remaining();
auto nextWindow = baseWindow + 1 /* time.Second */ ;
auto expires = 0;
// have fewer expires than the total revoke rate so piled up leases
// don't consume the entire revoke limit
auto targetExpiresPerSecond = (3 * leaseRevokeRate) / 4;
foreach (l; leases)
{
	auto remaining = l.Remaining();
	if (remaining > nextWindow)
	{
		baseWindow = remaining;
		nextWindow = baseWindow + 1 /* time.Second */ ;
		expires = 1;
		continue;
	}
	expires++;
	if (expires <= targetExpiresPerSecond)
	{
		continue;
	}
	auto rateDelay = float64(1 /* time.Second */ ) * (
			float64(expires) / float64(targetExpiresPerSecond));
	// If leases are extended by n seconds, leases n seconds ahead of the
	// base window should be extended by only one second.
	rateDelay -= float64(remaining - baseWindow);
	auto delay = /* time.Duration */ (rateDelay);
	nextWindow = baseWindow + delay;
	l.refresh(delay + extend);
	LeaseWithTime item = {id:
	l.ID, time : l.expiry /* .UnixNano() */ };
this.leaseHeap.Push(item);
this.scheduleCheckpointIfNeeded(l);
}
}

void Demote()
{
	_mutex.lock();
	scope (exit)
		_mutex.unlock();

	// set the expiries of all leases to forever
	foreach (LeaseID k, Lease l; this.leaseMap)
	{
		l.forever();
	}

	this.clearScheduledLeasesCheckpoints();

	// if (this.demotec != null)
	// {
	// 	close(this.demotec);
	// 	this.demotec = null;
	// }
}

// Attach attaches items to the lease with given ID. When the lease
// expires, the attached items will be automatically removed.
// If the given lease does not exist, an error will be returned.
string Attach(LeaseID id, LeaseItem[] items)
{
	_mutex.lock();
	scope (exit)
		_mutex.unlock();

	auto l = this.leaseMap[id];
	if (l is null)
	{
		return ErrLeaseNotFound;
	}

	l._mutex.writer().lock();
	foreach (it; items)
	{
		l.itemSet[it] = new Object();
		this.itemMap[it] = id;
	}
	l._mutex.writer().unlock();
	return null;
}

LeaseID GetLease(LeaseItem item)
{
	this._mutex.lock();
	auto id = this.itemMap[item];
	this._mutex.unlock();
	return id;
}

// Detach detaches items from the lease with given ID.
// If the given lease does not exist, an error will be returned.
string Detach(LeaseID id, LeaseItem[] items)
{
	_mutex.lock();
	scope (exit)
		_mutex.unlock();

	auto l = this.leaseMap[id];
	if (l is null)
	{
		return ErrLeaseNotFound;
	}

	l._mutex.writer().lock();
	foreach (it; items)
	{
		// delete(l.itemSet, it);
		l.itemSet.remove(it);
		// delete(this.itemMap, it);
		this.itemMap.remove(it);
	}
	l._mutex.writer().unlock();
	return null;
}

void Recover( /* backend.Backend b, */ RangeDeleter rd)
{
	_mutex.lock();
	scope (exit)
		_mutex.unlock();

	// this.b = b;
	this.rd = rd;
	this.leaseMap.clear;
	this.itemMap.clear;
	this.initAndRecover();
}

//  ExpiredLeasesC() <-chan []*Lease {
// 	return this.expiredC;
// }

//  void Stop() {
// 	close(this.stopC)
// 	<-this.doneC
// }

// void runLoop() {
// 	defer close(this.doneC);

// 	for {
// 		this.revokeExpiredLeases()
// 		this.checkpointScheduledLeases()

// 		select {
// 		case <-time.After(500 * time.Millisecond):
// 		case <-this.stopC:
// 			return
// 		}
// 	}
// }

// revokeExpiredLeases finds all leases past their expiry and sends them to epxired channel for
// to be revoked.
void revokeExpiredLeases()
{
	Lease[] ls;

	// rate limit
	auto revokeLimit = leaseRevokeRate / 2;

	this._mutex.lock();
	if (this.isPrimary())
	{
		ls = this.findExpiredLeases(revokeLimit);
	}
	this._mutex.unlock();

	if ((ls.length) != 0)
	{
		///@gxc
		implementationMissing();
		// select
		// {
		// case  <  - this.stopC : return case this.expiredC <  - ls : default :  // the receiver of expiredC is probably busy handling
		// 	// other stuff
		// 	// let's try this next time after 500ms
		// }
	}
}

// checkpointScheduledLeases finds all scheduled lease checkpoints that are due and
// submits them to the checkpointer to persist them to the consensus log.
void checkpointScheduledLeases()
{
	LeaseCheckpoint[] cps;

	// rate limit
	for (int i = 0; i < leaseCheckpointRate / 2; i++)
	{
		_mutex.lock();
		if (this.isPrimary())
		{
			cps = this.findDueScheduledCheckpoints(maxLeaseCheckpointBatchSize);
		}
		this._mutex.unlock();

		if ((cps.length) != 0)
		{
			LeaseCheckpointRequest req = new LeaseCheckpointRequest();
			req.checkpoints = cps;
			this.cp( /* context.Background(),  */ req);
		}
		if ((cps.length) < maxLeaseCheckpointBatchSize)
		{
			return;
		}
	}
}

void clearScheduledLeasesCheckpoints()
{
	this.leaseCheckpointHeap.clear();
}

// expireExists returns true if expiry items exist.
// It pops only when expiry item exists.
// "next" is true, to indicate that it may exist in next attempt.
Tuple!(Lease, bool, bool) expireExists()
{
	Tuple!(Lease, bool, bool) result;
	if (this.leaseHeap.length() == 0)
	{
		result[0] = null;
		result[1] = false;
		result[2] = false;
		return result;
	}

	auto item = this.leaseHeap.get!LeaseWithTime(0);
	result[0] = this.leaseMap[item.id];
	if (result[0] is null)
	{
		// lease has expired or been revoked
		// no need to revoke (nothing is expiry)
		this.leaseHeap.Pop!LeaseWithTime(); // O(log N)
		result[1] = false;
		result[2] = true;
		return result;
	}

	if (Instant.now().getEpochSecond() < item.time) /* expiration time */
	{
		// Candidate expirations are caught up, reinsert this item
		// and no need to revoke (nothing is expiry)
		result[1] = false;
		result[2] = false;
		return result;
	}
	// if the lease is actually expired, add to the removal list. If it is not expired, we can ignore it because another entry will have been inserted into the heap

	this.leaseHeap.Pop!LeaseWithTime(); // O(log N)
	result[1] = true;
	result[2] = false;
	return result;
}

// findExpiredLeases loops leases in the leaseMap until reaching expired limit
// and returns the expired leases that needed to be revoked.
Lease[] findExpiredLeases(int limit)
{
	Lease[] leases;

	while (1)
	{
		Tuple!(Lease, bool, bool) result = this.expireExists();
		Lease l = result[0];
		bool ok = result[1];
		bool next = result[2];
		if (!ok && !next)
		{
			break;
		}
		if (!ok)
		{
			continue;
		}
		if (next)
		{
			continue;
		}

		if (l.expired())
		{
			leases ~= l;

			// reach expired limit
			if ((leases.length) == limit)
			{
				break;
			}
		}
	}

	return leases;
}

void scheduleCheckpointIfNeeded(Lease lease)
{
	if (this.cp is null)
	{
		return;
	}

	if (lease.RemainingTTL() > int64(this.checkpointInterval/* .Seconds() */))
	{
		// if (this.lg != null) {
		// 	this.lg.Debug("Scheduling lease checkpoint",
		// 		zap.Int64("leaseID", int64(lease.ID)),
		// 		zap.Duration("intervalSeconds", this.checkpointInterval),
		// 	);
		// }
		LeaseWithTime item = {
		id:
			lease.ID, time : Instant.now().getEpochSecond() + (this.checkpointInterval) /* .UnixNano() */ 
		};
		this.leaseCheckpointHeap.Push(item);
	}
}

LeaseCheckpoint[] findDueScheduledCheckpoints(int checkpointLimit)
{
	if (this.cp is null)
	{
		return null;
	}

	auto now = Instant.now().getEpochSecond();
	LeaseCheckpoint[] cps;
	while (this.leaseCheckpointHeap.length() > 0 && (cps.length) < checkpointLimit)
	{
		auto lt = this.leaseCheckpointHeap.get!LeaseWithTime(0);
		if (lt.time > now /* .UnixNano() */ )
		{
			return cps;
		}
		this.leaseCheckpointHeap.Pop!LeaseWithTime();
		Lease l = this.leaseMap[lt.id];
		bool ok;
		if (l is null)
		{
			continue;
		}
		if (!(now < (l.expiry)))
		{
			continue;
		}
		auto remainingTTL = int64((l.expiry - (now)/* .Seconds() */));
		if (remainingTTL >= l.ttl)
		{
			continue;
		}
		// if (this.lg != null)
		{
			logDebug("Checkpointing lease --", "leaseID : ",
					lt.id, "remainingTTL : ", remainingTTL);
		}
		LeaseCheckpoint item = new LeaseCheckpoint();
		item.ID = int64(lt.id);
		item.remainingTTL = remainingTTL;
		cps ~= item;
	}
	return cps;
}

void initAndRecover()
{
	///@gxc
	implementationMissing();
	// 	auto tx = this.b.BatchTx();
	// 	tx.Lock();

	// 	tx.UnsafeCreateBucket(leaseBucketName);
	// 	_, vs :  = tx.UnsafeRange(leaseBucketName, int64ToBytes(0), int64ToBytes(math.MaxInt64), 0);
	// 	// TODO: copy vs and do decoding outside tx lock if lock contention becomes an issue.
	// 	for (int i = 0; i < vs.length; i++)
	// 	{
	// 		leasepb.Lease lpb;
	// 		auto err = lpb.Unmarshal(vs[i]);
	// 		if (err != null)
	// 		{
	// 			tx.Unlock();
	// 			panic("failed to unmarshal lease proto item");
	// 		}
	// 		auto ID = LeaseID(lpb.ID);
	// 		if (lpb.TTL < this.minLeaseTTL)
	// 		{
	// 			lpb.TTL = this.minLeaseTTL;
	// 		}
	// 		this.leaseMap[ID] = Lease
	// 		{
	// 		ID:
	// 			ID, ttl : lpb.TTL, // itemSet will be filled in when recover key-value pairs
	// 				// set expiry to forever, refresh when promoted
	// 		itemSet : make(map[LeaseItem]struct
	// 					{
	// 				}
	// ), expiry : forever, revokec : make(chan struct
	// 				{
	// 			}),
	// 		};
	// 	}
	// this.leaseHeap.Init();
	// this.leaseCheckpointHeap.Init();
	// tx.Unlock();

	// this.b.ForceCommit();
}

}

struct LessorConfig
{
	int64 MinLeaseTTL;
	long CheckpointInterval;
}

Lessor NewLessor(LessorConfig cfg)
{
	return newLessor(cfg);
}

Lessor newLessor(LessorConfig cfg)
{
	auto checkpointInterval = cfg.CheckpointInterval;
	if (checkpointInterval == 0)
	{
		checkpointInterval = 5 * 60;
	}
	// l := &lessor{
	// 	leaseMap:            make(map[LeaseID]*Lease),
	// 	itemMap:             make(map[LeaseItem]LeaseID),
	// 	leaseHeap:           make(LeaseQueue, 0),
	// 	leaseCheckpointHeap: make(LeaseQueue, 0),
	// 	b:                   b,
	// 	minLeaseTTL:         cfg.MinLeaseTTL,
	// 	checkpointInterval:  checkpointInterval,
	// 	// expiredC is a small buffered chan to avoid unnecessary blocking.
	// 	expiredC: make(chan []*Lease, 16),
	// 	stopC:    make(chan struct{}),
	// 	doneC:    make(chan struct{}),
	// 	lg:       lg,
	// }
	auto l = new Lessor(cfg.MinLeaseTTL, checkpointInterval);
	l.initAndRecover(); // go l.runLoop()

	return l;
}

alias leasesByExpiry = Lease[];

int Len(leasesByExpiry le)
{
	return cast(int)(le.length);
}

bool Less(leasesByExpiry le, int i, int j)
{
	return le[i].Remaining() < le[j].Remaining();
}

void Swap(leasesByExpiry le, int i, int j)
{
	auto temp = le[i];
	le[i] = le[j];
	le[j] = temp;
}

class Lease
{
	LeaseID ID;
	int64 ttl; // time to live of the lease in seconds
	int64 remainingTTL; // remaining time to live in seconds, if zero valued it is considered unset and the full ttl should be used
	// expiryMu protects concurrent accesses to expiry
	Mutex expiryMu;
	// expiry is time when lease should expire. no expiration when expiry.IsZero() is true
	long expiry;

	// mu protects concurrent accesses to itemSet
	ReadWriteMutex _mutex;
	Object[LeaseItem] itemSet;
	// revokec chan struct{}

	bool expired()
	{
		return this.Remaining() <= 0;
	}

	void persistTo( /* backend.Backend b */ )
	{
		implementationMissing();
		// auto key = int64ToBytes(int64(this.ID));

		// auto lpb = new leasepb.Lease();
		// lpb.ID = int64(this.ID);
		// lpb.TTL = this.ttl;
		// lpb.RemainingTTL = this.remainingTTL;

		// auto val = lpb.Marshal();
		// if (val is null)
		// {
		// 	panic("failed to marshal lease proto item");
		// }

		// b.BatchTx().Lock();
		// b.BatchTx().UnsafePut(leaseBucketName, key, val);
		// b.BatchTx().Unlock();
	}

	// TTL returns the TTL of the Lease.
	int64 TTL()
	{
		return this.ttl;
	}

	// RemainingTTL returns the last checkpointed remaining TTL of the lease.
	// TODO(jpbetz): do not expose this utility method
	int64 RemainingTTL()
	{
		if (this.remainingTTL > 0)
		{
			return this.remainingTTL;
		}
		return this.ttl;
	}

	// refresh refreshes the expiry of the lease.
	void refresh(long extend)
	{
		auto newExpiry = Instant.now().getEpochSecond() + (extend + /* time.Duration */ (
					this.RemainingTTL()) * 1);
		this.expiryMu.lock();
		scope (exit)
			this.expiryMu.unlock();
		this.expiry = newExpiry;
	}

	// forever sets the expiry of lease to be forever.
	void forever()
	{
		this.expiryMu.lock();
		scope (exit)
			this.expiryMu.unlock();
		this.expiry = FOREVER;
	}

	// Keys returns all the keys attached to the lease.
	string[] Keys()
	{
		this._mutex.reader.lock();
		string[] keys;
		foreach (k, v; this.itemSet)
		{
			keys ~= k.Key;
		}
		this._mutex.reader.unlock();
		return keys;
	}

	// Remaining returns the remaining time of the lease.
	long Remaining()
	{
		this.expiryMu.lock();
		scope (exit)
			this.expiryMu.unlock();
		if (this.expiry == long.init)
		{
			return long.max;
		}
		return /* time.Until */(this.expiry - Instant.now().getEpochSecond());
	}

}

struct LeaseItem
{
	string Key;
}

byte[] int64ToBytes(int64 n)
{
	// bytes := make([]byte, 8)
	// binary.BigEndian.PutUint64(bytes, uint64(n))
	byte[] bytes;
	auto d = nativeToBigEndian(n);
	foreach(b; d) {
		bytes ~= cast(byte)b;
	}
	return bytes ;
}

// FakeLessor is a fake implementation of Lessor interface.
// Used for testing only.
// type FakeLessor struct{}

// func (fl *FakeLessor) SetRangeDeleter(dr RangeDeleter) {}

// func (fl *FakeLessor) SetCheckpointer(cp Checkpointer) {}

// func (fl *FakeLessor) Grant(id LeaseID, ttl int64) (*Lease, error) { return null, null }

// func (fl *FakeLessor) Revoke(id LeaseID) error { return null }

// func (fl *FakeLessor) Checkpoint(id LeaseID, remainingTTL int64) error { return null }

// func (fl *FakeLessor) Attach(id LeaseID, items []LeaseItem) error { return null }

// func (fl *FakeLessor) GetLease(item LeaseItem) LeaseID            { return 0 }
// func (fl *FakeLessor) Detach(id LeaseID, items []LeaseItem) error { return null }

// func (fl *FakeLessor) Promote(extend time.Duration) {}

// func (fl *FakeLessor) Demote() {}

// func (fl *FakeLessor) Renew(id LeaseID) (int64, error) { return 10, null }

// func (fl *FakeLessor) Lookup(id LeaseID) *Lease { return null }

// func (fl *FakeLessor) Leases() []*Lease { return null }

// func (fl *FakeLessor) ExpiredLeasesC() <-chan []*Lease { return null }

// func (fl *FakeLessor) Recover(b backend.Backend, rd RangeDeleter) {}

// func (fl *FakeLessor) Stop() {}
