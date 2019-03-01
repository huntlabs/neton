module neton.rpcservice.KVService;

import etcdserverpb.kv;
import etcdserverpb.rpc;
import etcdserverpb.rpcrpc;
import grpc;
import hunt.logging;
import neton.server.NetonRpcServer;
import neton.util.Future;
import neton.rpcservice.Command;
import std.stdio;

class KVService : KVBase
{

    override Status Range(RangeRequest req, ref RangeResponse response)
    {
        logDebug("--------------");
        if(req.key.length == 0)
        {
            logError("get key is null");
            return new Status(StatusCode.INVALID_ARGUMENT);
        }

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

        return Status.OK;
    }

    override Status Put(PutRequest req, ref PutResponse response)
    {
        logDebug("--------------");

        if(req.key.length == 0)
        {
            logError("put key is null");
            return new Status(StatusCode.INVALID_ARGUMENT);
        }

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
        if(response !is null)
            return Status.OK;
        else
            return new Status(StatusCode.INVALID_ARGUMENT);
    }

    override Status DeleteRange(DeleteRangeRequest req, ref DeleteRangeResponse response)
    {
        logDebug("--------------");

        if(req.key.length == 0)
        {
            logError("delete key is null");
            return new Status(StatusCode.INVALID_ARGUMENT);
        }

        auto f = new Future!(DeleteRangeRequest, DeleteRangeResponse)(req);

        RpcRequest rreq;
        rreq.CMD = RpcReqCommand.DeleteRangeRequest;
        rreq.Key = cast(string)(req.key);
        rreq.Hash = f.toHash();
        NetonRpcServer.instance().Propose(rreq, f);
        logDebug("waiting ..... : ", rreq);
        response = f.get();
        logDebug("waiting done.....");

        if(response !is null)
            return Status.OK;
        else
            return new Status(StatusCode.INVALID_ARGUMENT);
    }

}
