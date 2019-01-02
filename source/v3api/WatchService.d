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

struct WatchInfo
{
    bool created = false;
    long watchId = 0;
    ResponseHeader header;
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
                    watchReq._createRequest.watchId);
            if (watchReq.requestUnionCase() == WatchRequest.RequestUnionCase.createRequest)
            {
                auto f = new Future!(ServerReaderWriter!(WatchRequest, WatchResponse), WatchInfo)(
                        rw);
                if (this.winfo.watchId == 0)
                {
                    WatchInfo info;
                    auto header = new ResponseHeader();
                    header.clusterId = 1;
                    header.memberId = 1;
                    header.raftTerm = 1;
                    header.revision = 1;
                    info.created = true;
                    info.watchId = 100;
                    info.header = header;
                    this.winfo = info;
                    f.setResData(info);
                }
                else
                    f.setResData(this.winfo);
   
                RpcRequest rreq;
                rreq.CMD = RpcReqCommand.WatchRequest;
                rreq.Key = cast(string)(watchReq._createRequest.key);
                rreq.Hash = f.toHash();
                NetonRpcServer.instance().ReadIndex(rreq, f);
            }

        }
        logWarning("watch service end : ", this.toHash());
        return Status.OK;
    }

}
