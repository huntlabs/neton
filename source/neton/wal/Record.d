module neton.wal.Record;

enum WalType {
    metadataType  = 0,
	entryType     = 1,
	stateType     = 2,
	crcType       = 3,
	snapshotType  = 4,

	// warnSyncDuration is the amount of time allotted to an fsync before
	// logging a warning
	warnSyncDuration = 5,
    recordType = 6 ,
}

struct Record {
	 WalType type;
	 string data;
}

struct WalSnapshot {
	 ulong index;
	 ulong term;
}
