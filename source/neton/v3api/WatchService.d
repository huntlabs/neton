module neton.v3api.WatchService;

import neton.etcdserverpb.kv;
import neton.etcdserverpb.rpc;
import neton.etcdserverpb.rpcrpc;
import grpc;
import hunt.logging;
import neton.server.NetonRpcServer;
import neton.util.Future;
import neton.v3api.Command;
import std.stdio;
import hunt.util.serialize;

struct WatchInfo
{
    bool created = false;
    long watchId = 0;
    ResponseHeader header;
}

class WatchService : WatchBase
{
    private WatchInfo winfo;
    private int lastWatchId = 0;

    override Status Watch(ServerReaderWriter!(WatchRequest, WatchResponse) rw)
    {
        WatchRequest watchReq;

        while (rw.read(watchReq))
        {
            logDebug("watch -----> : ", watchReq.requestUnionCase(), " ID : ",
                    watchReq._createRequest.watchId," last watchid : ",this.lastWatchId);
            if (watchReq.requestUnionCase() == WatchRequest.RequestUnionCase.createRequest)
            {
                auto f = new Future!(ServerReaderWriter!(WatchRequest, WatchResponse), WatchInfo)(
                        rw);
                WatchResponse respon = new WatchResponse();
                respon.created = true;
                respon.watchId = ++this.lastWatchId;
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
                rreq.Hash = f.toHash();
                WatchInfo info;
                info.created = true;
                info.watchId = respon.watchId;
                info.header = header;
                f.setResData(info);
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
                rreq.Hash = f.toHash();
                WatchInfo info;
                info.created = false;
                info.watchId = respon.watchId;
                info.header = header;
                f.setResData(info);
                NetonRpcServer.instance().Propose(rreq, f);
            }
        }
        logWarning("watch service end : ", this.toHash());
        return Status.OK;
    }

}
