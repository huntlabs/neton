module neton.rpcservice.Command;

enum RpcReqCommand
{
	///KV request
	RangeRequest = 0,
	PutRequest = 1,
	DeleteRangeRequest = 2,
	/// watch request
	WatchRequest = 3,
	WatchCancelRequest = 4,
	/// lease requst
	LeaseGenIDRequest = 5,
	LeaseGrantRequest = 6,
	LeaseRevokeRequest = 7,
	LeaseTimeToLiveRequest = 8,
	LeaseLeasesRequest = 9,
	LeaseKeepAliveRequest = 10,
	///config request
	ConfigRangeRequest = 11,
	ConfigPutRequest = 12,
	ConfigDeleteRangeRequest = 13,
	///registry request
	RegistryRangeRequest = 14,
	RegistryPutRequest = 15,
	RegistryDeleteRangeRequest = 16,
};

struct RpcRequest
{
	RpcReqCommand CMD;
	string Key;
	string Value;
	size_t Hash;
	long LeaseID;
	long TTL;
};