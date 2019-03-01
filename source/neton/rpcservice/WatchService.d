module neton.rpcservice.WatchService;

import etcdserverpb.kv;
import etcdserverpb.rpc;
import etcdserverpb.rpcrpc;
import grpc;
import hunt.logging;
import neton.server.NetonRpcServer;
import neton.util.Future;
import neton.rpcservice.Command;
import std.stdio;
import hunt.util.Serialize;
import core.sync.mutex;
import std.conv;

struct WatchInfo
{
    bool created = false;
    long watchId = 0;
    ResponseHeader header;
}

__gshared size_t WATCH_ID = 0;
__gshared Mutex _gmutex;

shared static this()
{
    _gmutex = new Mutex();
}

size_t plusWatchId()
{
    synchronized( _gmutex )
    {
        ++WATCH_ID;
    }
    return WATCH_ID;
}

class WatchService : WatchBase
{

    override Status Watch(ServerReaderWriter!(WatchRequest, WatchResponse) rw)
    {
        logDebug("--------------");

        WatchRequest watchReq;

        while (rw.read(watchReq))
        {
            logDebug("watch -----> : ", watchReq.requestUnionCase(), " last watchid : ",WATCH_ID);
            if (watchReq.requestUnionCase() == WatchRequest.RequestUnionCase.createRequest)
            {
                auto f = new Future!(ServerReaderWriter!(WatchRequest, WatchResponse), WatchInfo)(
                        rw);
                WatchResponse respon = new WatchResponse();
                respon.created = true;
                respon.watchId = plusWatchId();
                auto header = new ResponseHeader();
                header.clusterId = 1;
                header.memberId = 1;
                header.raftTerm = 1;
                header.revision = 1;
                respon.header = header;
                
                rw.write(respon);

                RpcRequest rreq;
                rreq.CMD = RpcReqCommand.WatchRequest;
                rreq.Key = cast(string)(watchReq._createRequest.key);
                rreq.Value = respon.watchId.to!string;
                rreq.Hash = this.toHash() + respon.watchId;
                WatchInfo info;
                info.created = true;
                info.watchId = respon.watchId;
                info.header = header;
                f.setExtraData(info);
                NetonRpcServer.instance().ReadIndex(rreq, f);
            }
            else if (watchReq.requestUnionCase() == WatchRequest.RequestUnionCase.cancelRequest)
            {
                logDebug("watch cancel ID : ",watchReq._cancelRequest.watchId);

                auto f = new Future!(ServerReaderWriter!(WatchRequest, WatchResponse), WatchInfo)(
                        rw);
                auto watchID = watchReq._cancelRequest.watchId;
                auto header = new ResponseHeader();
                header.clusterId = 1;
                header.memberId = 1;
                header.raftTerm = 1;
                header.revision = 1;
                
                RpcRequest rreq;
                rreq.CMD = RpcReqCommand.WatchCancelRequest;
                rreq.Value = watchID.to!string;
                rreq.Hash = this.toHash() + watchID;
                WatchInfo info;
                info.created = false;
                info.watchId = watchID;
                info.header = header;
                f.setExtraData(info);
                NetonRpcServer.instance().Propose(rreq, f);
            }
        }
        logWarning("watch service end : ", this.toHash());
        return Status.OK;
    }

}
