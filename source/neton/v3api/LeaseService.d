module neton.v3api.LeaseService;

import neton.etcdserverpb.kv;
import neton.etcdserverpb.rpc;
import neton.etcdserverpb.rpcrpc;
import grpc;
import hunt.logging;
import neton.server.NetonRpcServer;
import neton.util.Future;
import neton.v3api.Command;
import std.stdio;

class LeaseService : LeaseBase
{
    override Status LeaseGrant(LeaseGrantRequest req, ref LeaseGrantResponse response)
    {
        auto f = new Future!(LeaseGrantRequest, LeaseGrantResponse)(req);

        RpcRequest rreq;
        if (req.ID == 0)
            rreq.CMD = RpcReqCommand.LeaseGenIDRequest;
        else
            rreq.CMD = RpcReqCommand.LeaseGrantRequest;
        rreq.LeaseID = req.ID;
        rreq.TTL = req.TTL;
        rreq.Hash = f.toHash();
        NetonRpcServer.instance().Propose(rreq, f);
        logDebug("waiting ..... : ", rreq);
        response = f.get();
        logDebug("waiting done.....");

        return Status.OK;
    }

    override Status LeaseRevoke(LeaseRevokeRequest req, ref LeaseRevokeResponse response)
    {
        auto f = new Future!(LeaseRevokeRequest, LeaseRevokeResponse)(req);

        RpcRequest rreq;
        rreq.CMD = RpcReqCommand.LeaseRevokeRequest;
        rreq.LeaseID = req.ID;
        rreq.Hash = f.toHash();
        NetonRpcServer.instance().Propose(rreq, f);
        logDebug("waiting ..... : ", rreq);
        response = f.get();
        logDebug("waiting done.....");

        return Status.OK;
    }

    override Status LeaseKeepAlive(ServerReaderWriter!(LeaseKeepAliveRequest,
            LeaseKeepAliveResponse) rw)
    {
        LeaseKeepAliveRequest req;
        while (rw.read(req))
        {
            logDebug("LeaseKeepAlive ----->  ID : ", req.ID);

            auto f = new Future!(ServerReaderWriter!(LeaseKeepAliveRequest, LeaseKeepAliveResponse), LeaseKeepAliveResponse)(rw);

            RpcRequest rreq;
            rreq.CMD = RpcReqCommand.LeaseKeepAliveRequest;
            rreq.LeaseID = req.ID;
            rreq.Hash = f.toHash();
            NetonRpcServer.instance().Propose(rreq, f);
        }
        logWarning("LeaseKeepAlive service end : ", this.toHash());

        return Status.OK;
    }

    override Status LeaseTimeToLive(LeaseTimeToLiveRequest req, ref LeaseTimeToLiveResponse response)
    {
        auto f = new Future!(LeaseTimeToLiveRequest, LeaseTimeToLiveResponse)(req);

        RpcRequest rreq;
        rreq.CMD = RpcReqCommand.LeaseTimeToLiveRequest;
        rreq.LeaseID = req.ID;
        rreq.Hash = f.toHash();
        NetonRpcServer.instance().Propose(rreq, f);
        logDebug("waiting ..... : ", rreq);
        response = f.get();
        logDebug("waiting done.....");

        return Status.OK;
    }

    override Status LeaseLeases(LeaseLeasesRequest req, ref LeaseLeasesResponse response)
    {
        auto f = new Future!(LeaseLeasesRequest, LeaseLeasesResponse)(req);

        RpcRequest rreq;
        rreq.CMD = RpcReqCommand.LeaseLeasesRequest;
        rreq.Hash = f.toHash();
        NetonRpcServer.instance().Propose(rreq, f);
        logDebug("waiting ..... : ", rreq);
        response = f.get();
        logDebug("waiting done.....");

        return Status.OK;
    }

}
