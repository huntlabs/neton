module v3api.Command;

enum RpcReqCommand
{
	RangeRequest = 0,
	PutRequest = 1,
	DeleteRangeRequest = 2,
	WatchRequest = 3,

};

struct RpcRequest
{
	RpcReqCommand CMD;
	string Key;
	string Value;
	size_t Hash;
};