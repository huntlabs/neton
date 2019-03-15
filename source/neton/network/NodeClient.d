module neton.network.NodeClient;

import hunt.logging;
import hunt.util.Serialize;
import hunt.raft;
import hunt.net;
import hunt.net.Session;
import core.stdc.string;
import neton.network.Interface;
import core.thread;

import std.bitmanip;
import std.stdint;

class NodeClient : MessageTransfer
{
    ///
    this(ulong srcID, ulong dstID)
    {
        this.srcID = srcID;
        this.dstID = dstID;
        client = NetUtil.createNetClient();
         auto conf = new hunt.net.Config.Config();
        CallBackHandler cbHandler = new CallBackHandler(dstID);
        conf.setHandler(cbHandler);
        client.setConfig(conf);
    }

    ///
    void connect(string host, int port, ConnectHandler handler = null)
    {
       
        client.connect(port, host, 0, (Result!NetSocket result) {
            if (handler !is null)
                handler(result);
            if (!result.failed())
                sock = result.result();
        });
    }

    ///
    void write(Message msg)
    {
        if (sock is null)
        {
            logWarning(srcID, " not connect now. ", dstID);
            return;
        }

        // logDebug(srcID , " sendto " , dstID , " "  , msg);
        ubyte[] data = cast(ubyte[]) serialize(msg);
        int len = cast(int) data.length;
        ubyte[4] head = nativeToBigEndian(len);

        sock.write(head ~ data);
    }

    void close()
    {
        sock.close();
    }

private:
    ulong srcID;
    ulong dstID;
    NetClient client;
    NetSocket sock = null;
}

import neton.server.NetonConfig;
import neton.server.PeerServers;
import std.conv;

class CallBackHandler : hunt.net.Handler.Handler
{
    private
    {
        ulong _peerId;
    }

    this(ulong peerid)
    {
        _peerId = peerid;
    }
    
    override void sessionOpened(Session session)
    {
        logInfo("open node client -----");
    }

    override void sessionClosed(Session session)
    {
        logInfo("close node client -----");
        auto conf = NetonConfig.instance().getConf(_peerId);
        PeerServers.instance().addPeer(_peerId,conf.ip ~ ":" ~ to!string(conf.nodeport) );
    }

    override void messageReceived(Session session, Object message){}

    override void exceptionCaught(Session session, Exception t){}

    override void failedOpeningSession(int sessionId, Exception t)
    {
    }

    override void failedAcceptingSession(int sessionId, Exception t)
    {
    }
}
