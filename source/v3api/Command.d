module v3api.Command;

enum RpcReqCommand
{
	RangeRequest = 0,
	PutRequest = 1,
	DeleteRangeRequest = 2,
	WatchRequest = 3,
	LeaseGenIDRequest = 4,
	LeaseGrantRequest = 5,
	LeaseRevokeRequest = 6,
	LeaseTimeToLiveRequest = 7,
	LeaseLeasesRequest = 8,
	LeaseKeepAliveRequest = 9,
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