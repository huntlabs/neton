module v3api.WatchService;

import etcdserverpb.kv;
import etcdserverpb.rpc;
import etcdserverpb.rpcrpc;
import grpc;
import hunt.logging;
import server.NetonRpcServer;
import util.Future;
import v3api.Command;
import std.stdio;
import hunt.util.serialize;

class WatchService : WatchBase
{

    override Status Watch(ServerReaderWriter!(WatchRequest, WatchResponse) rw)
    {
        WatchRequest watchReq;
        while (rw.read(watchReq))
        {
            logDebug("watch -----> : ", watchReq.requestUnionCase(), " ID : ",watchReq._createRequest.watchId);
            if (watchReq.requestUnionCase() == WatchRequest.RequestUnionCase.createRequest)
            {
                auto f = new Future!(ServerReaderWriter!(WatchRequest, WatchResponse), WatchResponse)(rw);

                RpcRequest rreq;
                rreq.CMD = RpcReqCommand.WatchRequest;
                rreq.Key = cast(string)(watchReq._createRequest.key);
                rreq.Hash = f.toHash();
                NetonRpcServer.instance().ReadIndex(rreq, f);
            }

        }
        logWarning("watch service end : ",this.toHash());
        return Status.OK;
    }

}
