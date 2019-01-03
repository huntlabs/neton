module neton.v3api.KVService;

import neton.etcdserverpb.kv;
import neton.etcdserverpb.rpc;
import neton.etcdserverpb.rpcrpc;
import grpc;
import hunt.logging;
import neton.server.NetonRpcServer;
import neton.util.Future;
import neton.v3api.Command;
import std.stdio;

class KVService : KVBase
{

    override Status Range(RangeRequest req, ref RangeResponse response)
    {

        auto f = new Future!(RangeRequest, RangeResponse)(req);

        RpcRequest rreq;
        rreq.CMD = RpcReqCommand.RangeRequest;
        rreq.Key = cast(string)(req.key);
        rreq.Hash = f.toHash();
        NetonRpcServer.instance().ReadIndex(rreq, f);
        logDebug("waiting ..... : ", rreq);
        response = f.get();
        logDebug("waiting done..... : ",response);
        if(response is null)
            response = new RangeResponse();
        NetonRpcServer.instance().removeRpcHandler(rreq.Hash);

        return Status.OK;
    }

    override Status Put(PutRequest req, ref PutResponse response)
    {
        auto f = new Future!(PutRequest, PutResponse)(req);

        RpcRequest rreq;
        rreq.CMD = RpcReqCommand.PutRequest;
        rreq.Key = cast(string)(req.key);
        rreq.Value = cast(string)(req.value);
        rreq.LeaseID = req.lease;
        rreq.Hash = f.toHash();
        NetonRpcServer.instance().Propose(rreq, f);
        logDebug("waiting ..... : ", rreq);
        response = f.get();
        logDebug("waiting done.....");

        return Status.OK;
    }

    override Status DeleteRange(DeleteRangeRequest req, ref DeleteRangeResponse response)
    {
        auto f = new Future!(DeleteRangeRequest, DeleteRangeResponse)(req);

        RpcRequest rreq;
        rreq.CMD = RpcReqCommand.DeleteRangeRequest;
        rreq.Key = cast(string)(req.key);
        rreq.Hash = f.toHash();
        NetonRpcServer.instance().Propose(rreq, f);
        logDebug("waiting ..... : ", rreq);
        response = f.get();
        logDebug("waiting done.....");

        return Status.OK;
    }

}
