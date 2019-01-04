module neton.v3api.WatchService;

import etcdserverpb.kv;
import etcdserverpb.rpc;
import etcdserverpb.rpcrpc;
import grpc;
import hunt.logging;
import neton.server.NetonRpcServer;
import neton.util.Future;
import neton.v3api.Command;
import std.stdio;
import hunt.util.serialize;
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
    private WatchInfo winfo;

    override Status Watch(ServerReaderWriter!(WatchRequest, WatchResponse) rw)
    {
        WatchRequest watchReq;

        while (rw.read(watchReq))
        {
            logDebug("watch -----> : ", watchReq.requestUnionCase(), " ID : ",
                    watchReq._createRequest.watchId," last watchid : ",WATCH_ID);
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
                rreq.Hash = this.toHash();
                WatchInfo info;
                info.created = true;
                info.watchId = respon.watchId;
                info.header = header;
                f.setExtraData(info);
                NetonRpcServer.instance().ReadIndex(rreq, f);
            }
            else if (watchReq.requestUnionCase() == WatchRequest.RequestUnionCase.cancelRequest)
            {
                auto f = new Future!(ServerReaderWriter!(WatchRequest, WatchResponse), WatchInfo)(
                        rw);
                WatchResponse respon = new WatchResponse();
                respon.created = false;
                respon.watchId = watchReq._cancelRequest.watchId;
                auto header = new ResponseHeader();
                header.clusterId = 1;
                header.memberId = 1;
                header.raftTerm = 1;
                header.revision = 1;
                respon.header = header;
                
                RpcRequest rreq;
                rreq.CMD = RpcReqCommand.WatchCancelRequest;
                rreq.Value = respon.watchId.to!string;
                rreq.Hash = this.toHash();
                WatchInfo info;
                info.created = false;
                info.watchId = respon.watchId;
                info.header = header;
                f.setExtraData(info);
                NetonRpcServer.instance().Propose(rreq, f);
            }
        }
        logWarning("watch service end : ", this.toHash());
        return Status.OK;
    }

}
