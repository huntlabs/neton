module neton.network.NodeClient;

import neton.network.Interface;
import neton.server.NetonConfig;
import neton.server.PeerServers;

import hunt.Functions;
import hunt.logging;
import hunt.raft;
import hunt.net;
import hunt.util.Serialize;

import core.stdc.string;
import core.thread;

import std.conv;
import std.format;
import std.bitmanip;
import std.stdint;

alias AsyncConnectHandler = NetEventHandler!(AsyncResult!Connection);

class NodeClient : MessageTransfer
{
    private AsyncConnectHandler _connectionHandler;
    private Connection _conn;

    ///
    this(ulong srcID, ulong dstID)
    {
        this.srcID = srcID;
        this.dstID = dstID;
        client = NetUtil.createNetClient();

        client.setHandler(new class NetConnectionHandler {

            override void connectionOpened(Connection connection) {
                infof("Connection created: %s", connection.getRemoteAddress());
                _conn = connection;
                if(_connectionHandler !is null) {
                    _connectionHandler(succeededResult(connection));
                }
            }

            override void connectionClosed(Connection connection) {
                infof("Connection closed: %s", connection.getRemoteAddress());

                auto conf = NetonConfig.instance().getConf(dstID);
                PeerServers.instance().addPeer(dstID, conf.ip ~ ":" ~ to!string(conf.nodeport) );
            }

            override void messageReceived(Connection connection, Object message) {
                tracef("message type: %s", typeid(message).name);
                string str = format("data received: %s", message.toString());
                tracef(str);
            }

            override void exceptionCaught(Connection connection, Throwable t) {
                warning(t);
            }

            override void failedOpeningConnection(int connectionId, Throwable t) {
                warning(t);
                client.close();
                
                if(_connectionHandler !is null) {
                    _connectionHandler(failedResult!(Connection)(t));
                } 
            }

            override void failedAcceptingConnection(int connectionId, Throwable t) {
                warning(t);
            }
        });

        //  auto conf = new hunt.net.Config.Config();
        // CallBackHandler cbHandler = new CallBackHandler(dstID);
        // conf.setHandler(cbHandler);
        // client.setConfig(conf);
    }

    ///
    void connect(string host, int port, AsyncConnectHandler handler = null)
    {
        _connectionHandler = handler;
        client.connect(host, port);
    }

    ///
    void write(Message msg)
    {
        if (_conn is null)
        {
            logWarning(srcID, " not connect now. ", dstID);
            return;
        }

        // logDebug(srcID , " sendto " , dstID , " "  , msg);
        ubyte[] data = cast(ubyte[]) serialize(msg);
        int len = cast(int) data.length;
        ubyte[4] head = nativeToBigEndian(len);

        _conn.write(head ~ data);
    }

    void close()
    {
        // sock.close();
        client.close();
    }

private:
    ulong srcID;
    ulong dstID;
    NetClient client;
    // conn sock = null;
}


// class CallBackHandler : NetConnectionHandler
// {
//     private
//     {
//         ulong _peerId;
//     }

//     this(ulong peerid)
//     {
//         _peerId = peerid;
//     }
    
//     override void connectionOpened(Connection connection)
//     {
//         logInfo("open node client -----");
//     }

//     override void connectionClosed(Connection connection)
//     {
//         logInfo("close node client -----");
//         auto conf = NetonConfig.instance().getConf(_peerId);
//         PeerServers.instance().addPeer(_peerId,conf.ip ~ ":" ~ to!string(conf.nodeport) );
//     }

//     override void messageReceived(Connection connection, Object message){}

//     override void exceptionCaught(Connection connection, Throwable t) {}

//     override void failedOpeningConnection(int connectionId, Throwable t)
//     {
//     }

//     override void failedAcceptingConnection(int connectionId, Throwable t)
//     {
//     }
// }
