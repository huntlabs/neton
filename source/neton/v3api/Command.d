module neton.v3api.Command;

enum RpcReqCommand
{
	RangeRequest = 0,
	PutRequest = 1,
	DeleteRangeRequest = 2,
	WatchRequest = 3,
	WatchCancelRequest = 4,
	LeaseGenIDRequest = 5,
	LeaseGrantRequest = 6,
	LeaseRevokeRequest = 7,
	LeaseTimeToLiveRequest = 8,
	LeaseLeasesRequest = 9,
	LeaseKeepAliveRequest = 10,
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