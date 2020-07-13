module neton.server.NetonHttpServer;

import neton.network.NodeClient;
import neton.server.NetonConfig;
import neton.server.Health;
import neton.wal.Wal;
import neton.snap.SnapShotter;
import neton.wal.Record;
import neton.wal.Util;
import neton.store.Event;
import neton.store.Watcher;
import neton.store.Util;
import neton.store.Store;
import neton.network;

import hunt.io.ByteBuffer;
import hunt.logging.ConsoleLogger;
import hunt.util.Serialize;
import hunt.util.Timer;
import hunt.event.timer;
import hunt.event.EventLoop;
import hunt.event.timer.Common;
import hunt.raft;
import hunt.net;

import core.time;
import core.thread;
import core.sync.mutex;

import std.conv;
import std.file;
import std.stdio;
import std.json;
import std.algorithm.mutation;
import std.string;

enum defaultSnapCount = 10;
enum snapshotCatchUpEntriesN = 10000;

class NetonHttpServer : MessageReceiver
{
    private static NetonHttpServer _gserver;

    this()
    {
    }

    void publishSnapshot(Snapshot snap)
    {
        if (IsEmptySnap(snap))
            return;

        if (snap.Metadata.Index <= _appliedIndex)
        {
            logError("snapshot index [%d] should > progress.appliedIndex [%d] + 1",
                    snap.Metadata.Index, _appliedIndex);
        }

        _confState = snap.Metadata.CS;
        _snapshotIndex = snap.Metadata.Index;
        _appliedIndex = snap.Metadata.Index;
    }

    void saveSnap(Snapshot snap)
    {
        // must save the snapshot index to the WAL before saving the
        // snapshot to maintain the invariant that we only Open the
        // wal at previously-saved snapshot indexes.
        WalSnapshot walSnap = {
        index:
            snap.Metadata.Index, term : snap.Metadata.Term,
        };
        _wal.SaveSnapshot(walSnap);
        _snapshotter.SaveSnap(snap);

    }

    Entry[] entriesToApply(Entry[] ents)
    {
        if (ents.length == 0)
            return null;

        auto firstIdx = ents[0].Index;
        if (firstIdx > _appliedIndex + 1)
        {
            logError("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1",
                    firstIdx, _appliedIndex);
        }

        if (_appliedIndex - firstIdx + 1 < ents.length)
            return ents[_appliedIndex - firstIdx + 1 .. $];

        return null;
    }

    string makeJsonString(Event e)
    {
        if (e.error.length > 0)
            return e.error;
        JSONValue res;
        try
        {
            JSONValue j;
            j["action"] = e.action;
            j["netonIndex"] = Store.instance.Index();
            JSONValue node;
            node = e.getNodeValue();

            j["node"] = node;
            return j.toString();
        }
        catch (Exception e)
        {
            logError("catch %s", e.msg);
            res["error"] = e.msg;
        }
        return res.toString;
    }

    bool publishEntries(Entry[] ents)
    {
        bool iswatch = false;
        for (auto i = 0; i < ents.length; i++)
        {
            switch (ents[i].Type)
            {
            case EntryType.EntryNormal:
                if (ents[i].Data.length == 0)
                    break;

                RequestCommand command = unserialize!RequestCommand(cast(byte[]) ents[i].Data);

                string res;
                iswatch = false;
                switch (command.Method)
                {
                case RequestMethod.METHOD_GET:
                    {
                        //string value;
                        auto param = tryGetJsonFormat(command.Params);
                        logInfo("http GET param : ", param);
                        bool recursive = false;
                        if (param.type == JSONType.object && "recursive" in param)
                            recursive = param["recursive"].str == "true" ? true : false;
                        if (param.type == JSONType.object && ("wait" in param)
                                && param["wait"].str == "true")
                        {
                            ulong waitIndex = 0;
                            if ("waitIndex" in param)
                                waitIndex = to!ulong(param["waitIndex"].str);
                            auto w = Store.instance.Watch(command.Key, recursive, false, waitIndex);
                            w.setWatchId(command.Hash);
                            _watchers ~= w;
                            iswatch = true;
                        }
                        else
                        {
                            auto e = Store.instance.Get(command.Key, recursive, false);
                            //value = e.getNodeValue();
                            res = makeJsonString(e);
                        }

                    }
                    break;
                case RequestMethod.METHOD_PUT:
                    {
                        if (isRemained(command.Key))
                            res = "the " ~ command.Key ~ " is remained";
                        else
                        {
                            auto param = tryGetJsonFormat(command.Params);
                            logInfo("http PUT param : ", param);
                            bool dir = false;
                            if (param.type == JSONType.object && "dir" in param)
                                dir = param["dir"].str == "true" ? true : false;

                            if (dir)
                            {
                                auto e = Store.instance.CreateDir(command.Key);
                                res = makeJsonString(e);
                            }
                            else
                            {
                                string value;
                                if (param.type == JSONType.object && "value" in param)
                                    value = param["value"].str;
                                auto e = Store.instance.Set(command.Key, false, value);
                                res = makeJsonString(e);
                            }
                        }

                    }
                    break;
                case RequestMethod.METHOD_UPDATESERVICE:
                    {
                        auto param = tryGetJsonFormat(command.Params);
                        logInfo("http update service param : ", param);
                        {
                            if (param.type == JSONType.object)
                                auto e = Store.instance.Set(command.Key, false, param.toString);
                            //res = makeJsonString(e);
                        }

                    }
                    break;
                case RequestMethod.METHOD_DELETE:
                    {
                        auto param = tryGetJsonFormat(command.Params);
                        logInfo("http DELETE param : ", param);
                        bool recursive = false;
                        if (param.type == JSONType.object && "recursive" in param)
                            recursive = param["recursive"].str == "true" ? true : false;
                        auto e = Store.instance.Delete(command.Key, recursive);
                        res = makeJsonString(e);
                    }
                    break;
                case RequestMethod.METHOD_POST:
                    {
                        auto param = tryGetJsonFormat(command.Params);
                        logInfo("http post param : ", param);
                        bool recursive = false;
                        if (param.type == JSONType.object)
                        {
                            if (startsWith(command.Key, "/register"))
                            {
                                auto e = Store.instance.Register(param);
                                res = makeJsonString(e);
                                if (e.isOk())
                                {
                                    auto nd = e.getNodeValue();
                                    if ("key" in nd && "value" in nd)
                                    {
                                        // addHealthCheck(nd["key"].str,nd["value"]);
                                    }
                                }
                            }
                            else if (startsWith(command.Key, "/deregister"))
                            {
                                auto e = Store.instance.Deregister(param);
                                res = makeJsonString(e);
                                if (e.isOk)
                                {
                                    auto nd = e.getNodeValue();
                                    if ("key" in nd)
                                    {
                                        // removeHealthCheck(nd["key"].str);
                                    }
                                }
                            }
                            else
                            {
                                res = "can not sovle " ~ command.Key;
                            }
                        }
                        else
                        {
                            res = "can not sovle " ~ command.Key;
                        }

                    }
                    break;
                default:
                    break;
                }

                //if leader
                //if(_node.isLeader())
                {
                    auto http = (command.Hash in _request);
                    if (http != null)
                    {
                        logInfo("  http request params : ", http.params());
                        if (!iswatch)
                        {
                            http.do_response(res);
                            http.close();
                            _request.remove(command.Hash);
                        }

                        logInfo("  http request array length : ", _request.length);
                    }
                }
                // else
                // {
                // 	//logInfo("not leader handle http request : ",_ID);
                // }

                break;
                //next
            case EntryType.EntryConfChange:
                ConfChange cc = unserialize!ConfChange(cast(byte[]) ents[i].Data);
                _confState = _node.ApplyConfChange(cc);
                switch (cc.Type)
                {
                case ConfChangeType.ConfChangeAddNode:
                    if (cc.Context.length > 0)
                        addPeer(cc.NodeID, cc.Context);
                    break;
                case ConfChangeType.ConfChangeRemoveNode:
                    if (cc.NodeID == _ID)
                    {
                        logWarning(_ID, " I've been removed from the cluster! Shutting down.");
                        return false;
                    }
                    logWarning(_ID, " del node ", cc.NodeID);
                    delPeer(cc.NodeID);
                    break;
                default:
                    break;
                }
                break;
            default:

            }

            _appliedIndex = ents[i].Index;

        }

        return true;
    }

    void maybeTriggerSnapshot()
    {
        if (_appliedIndex - _snapshotIndex <= defaultSnapCount)
            return;

        logInfo("start snapshot [applied index: %d | last snapshot index: %d]",
                _appliedIndex, _snapshotIndex);

        auto data = loadSnapshot().Data;
        Snapshot snap;
        auto err = _storage.CreateSnapshot(_appliedIndex, &_confState, cast(string) data, snap);
        if (err != ErrNil)
        {
            logError(err);
        }

        saveSnap(snap);

        long compactIndex = 1;
        if (_appliedIndex > snapshotCatchUpEntriesN)
            compactIndex = _appliedIndex - snapshotCatchUpEntriesN;

        _storage.Compact(compactIndex);
        logInfo("compacted log at index ", compactIndex);
        _snapshotIndex = _appliedIndex;
    }

    void Propose(RequestCommand command, HttpBase h)
    {
        auto err = _node.Propose(cast(string) serialize(command));
        if (err != ErrNil)
        {
            logError("---------", err);
        }
        else
        {
            //logInfo("--------- request hash ",command.Hash);
            _request[command.Hash] = h;
        }
    }

    void Propose(RequestCommand command)
    {
        auto err = _node.Propose(cast(string) serialize(command));
        if (err != ErrNil)
        {
            logError("---------", err);
        }
    }

    void ReadIndex(RequestCommand command, HttpBase h)
    {
        _node.ReadIndex(cast(string) serialize(command));
        _request[command.Hash] = h;
    }

    void ProposeConfChange(ConfChange cc)
    {
        auto err = _node.ProposeConfChange(cc);
        if (err != ErrNil)
        {
            logError(err);
        }
    }

    Snapshot loadSnapshot()
    {
        auto snapshot = _snapshotter.loadSnap();

        return snapshot;
    }

    // openWAL returns a WAL ready for reading.
    void openWAL(Snapshot snapshot)
    {
        if (isEmptyDir(_waldir))
        {
            mkdir(_waldir);

            auto wal = new WAL(_waldir, null);

            if (wal is null)
            {
                logError("raftexample: create wal error ", _ID);
            }
            wal.Close();
        }

        WalSnapshot walsnap;

        walsnap.index = snapshot.Metadata.Index;
        walsnap.term = snapshot.Metadata.Term;

        logInfo("loading WAL at term ", walsnap.term, " and index ", walsnap.index);

        _wal = new WAL(_waldir, walsnap, true);

        if (_wal is null)
        {
            logError("raftexample: error loading wal ", _ID);
        }
    }

    // replayWAL replays WAL entries into the raft instance.
    void replayWAL()
    {
        logInfo("replaying WAL of member ", _ID);
        auto snapshot = loadSnapshot();
        openWAL(snapshot);

        //Snapshot *shot = null;
        HardState hs;
        Entry[] ents;
        byte[] metadata;

        _wal.ReadAll(metadata, hs, ents);

        _storage = new MemoryStorage();

        if (!IsEmptySnap(snapshot))
        {
            logInfo("******* exsit snapshot : ", snapshot);
            _storage.ApplySnapshot(snapshot);
            _confState = snapshot.Metadata.CS;
            _snapshotIndex = snapshot.Metadata.Index;
            _appliedIndex = snapshot.Metadata.Index;
        }

        _storage.setHadrdState(hs);

        _storage.Append(ents);
        if (ents.length > 0)
        {
            _lastIndex = ents[$ - 1].Index;
        }
    }

    void start(bool join)
    {
        _ID = NetonConfig.instance.selfConf.id;
        _snapdir = snapDirPath(_ID);
        _waldir = walDirPath(_ID);

        _mutex = new Mutex();

        if (!Exist(_snapdir))
        {
            mkdir(_snapdir);
        }
        _snapshotter = new Snapshotter(_snapdir);

        bool oldwal = isEmptyDir(_waldir);

        replayWAL();

        Config conf = new Config();

        Store.instance.Init(_ID , null);

        conf._ID = _ID;
        conf._ElectionTick = 10;
        conf._HeartbeatTick = 1;
        conf._storage = _storage;
        conf._MaxSizePerMsg = 1024 * 1024;
        conf._MaxInflightMsgs = 256;

        _buffer.length = 1024;

        Peer[] peers;
        Peer slf = {ID:
        NetonConfig.instance.selfConf.id};
        peers ~= slf;
        foreach (peer; NetonConfig.instance.peersConf)
        {
            Peer p = {ID:
            peer.id};
            peers ~= p;
        }

        if (!oldwal)
        {
            _node = new RawNode(conf);
        }
        else
        {
            if (join)
            {
                peers.length = 0;
            }
            logInfo("self conf : ", conf, "   peers conf : ", peers);
            _node = new RawNode(conf, peers);
        }

        // API server (HTTP)
        initApiServer();

        // Node server (TCP)
        initNodeServer();

        //
        foreach (peer; NetonConfig.instance.peersConf)
        {
            addPeer(peer.id, peer.ip ~ ":" ~ to!string(peer.nodeport));
        }

        EventLoop timerLoop = new EventLoop();

        new Timer(timerLoop, 100.msecs).onTick(&ready).start();

        new Timer(timerLoop, 100.msecs).onTick(
                &scanWatchers).start();

        new Timer(timerLoop, 1000.msecs).onTick(&onTimer).start();

        timerLoop.runAsync(-1);
    }

    private void initApiServer() {
        _http = NetUtil.createNetServer!(ThreadMode.Single)();

        _http.setHandler(new class NetConnectionHandler {
            private HttpBase _httpBase;

            override void connectionOpened(Connection connection) {
                infof("Connection created: %s", connection.getRemoteAddress());
                _httpBase = new HttpBase(connection, this.outer);
            }

            override void connectionClosed(Connection connection) {
                infof("Connection closed: %s", connection.getRemoteAddress());
                _httpBase.onClose();
            }

            override void messageReceived(Connection connection, Object message) {
                tracef("message type: %s", typeid(message).name);
                // string str = format("data received: %s", message.toString());
                // tracef(str);
                ByteBuffer buffer = cast(ByteBuffer)message;
                byte[] data = buffer.getRemaining();
                _httpBase.onRead(cast(ubyte[])data);
            }

            override void exceptionCaught(Connection connection, Throwable t) {
                warning(t);
            }

            override void failedOpeningConnection(int connectionId, Throwable t) {
                error(t);
            }

            override void failedAcceptingConnection(int connectionId, Throwable t) {
                error(t);
            }			
        });

        _http.listen("0.0.0.0", cast(int)NetonConfig.instance.selfConf.apiport);
    }

    private void initNodeServer() {
        // _server = new NetServer!(ServerHandler, MessageReceiver)(_ID, this);
        _server = NetUtil.createNetServer!(ThreadMode.Single)();

        _server.setHandler(new class NetConnectionHandler {
            private ServerHandler _serverHandler;

            override void connectionOpened(Connection connection) {
                infof("Connection created: %s", connection.getRemoteAddress());
                _serverHandler = new ServerHandler(this.outer);
            }

            override void connectionClosed(Connection connection) {
                infof("Connection closed: %s", connection.getRemoteAddress());
                _serverHandler.onClose();
            }

            override void messageReceived(Connection connection, Object message) {
                tracef("message type: %s", typeid(message).name);
                // string str = format("data received: %s", message.toString());
                // tracef(str);
                ByteBuffer buffer = cast(ByteBuffer)message;
                byte[] data = buffer.getRemaining();
                _serverHandler.onRead(cast(ubyte[])data);
            }

            override void exceptionCaught(Connection connection, Throwable t) {
                warning(t);
            }

            override void failedOpeningConnection(int connectionId, Throwable t) {
                error(t);
            }

            override void failedAcceptingConnection(int connectionId, Throwable t) {
                error(t);
            }			
        });

        _server.listen("0.0.0.0", cast(int)NetonConfig.instance.selfConf.nodeport);
    }

    bool addPeer(ulong ID, string data)
    {
        if (ID in _clients)
            return false;

        auto client = new NodeClient(this._ID, ID);
        string[] hostport = split(data, ":");
        // client.connect(hostport[0], to!int(hostport[1]), (Result!NetSocket result) {
        //     if (result.failed())
        //     {

        //         logWarning("connect fail --> : ", data);
        //         new Thread(() {
        //             Thread.sleep(dur!"seconds"(1));
        //             addPeer(ID, data);
        //         }).start();
        //         return;
        //     }
        //     _clients[ID] = client;
        //     logInfo(this._ID, " client connected ", hostport[0], " ", hostport[1]);
        //     // return true;
        // });

        client.connect(hostport[0], to!int(hostport[1]), (AsyncResult!Connection result) {
            if (result.failed())
            {

                logWarning("connect fail --> : ", data);
                new Thread(() {
                    Thread.sleep(dur!"seconds"(1));
                    addPeer(ID, data);
                }).start();
                return;
            }
            _clients[ID] = client;
            logInfo(this._ID, " client connected ", hostport[0], " ", hostport[1]);
            // return true;
        });

        return true;
    }

    bool delPeer(ulong ID)
    {
        if (ID !in _clients)
            return false;

        logInfo(_ID, " client disconnect ", ID);
        _clients[ID].close();
        _clients.remove(ID);

        return true;
    }

    void send(Message[] msg)
    {
        foreach (m; msg)
            _clients[m.To].write(m);
    }

    void step(Message msg)
    {
        _mutex.lock();
        scope (exit)
            _mutex.unlock();

        _node.Step(msg);
    }

    void onTimer(Object sender)
    {
        _mutex.lock();
        scope (exit)
            _mutex.unlock();

        _node.Tick();
    }

    void ready(Object sender)
    {
        _mutex.lock();
        scope (exit)
            _mutex.unlock();

        Ready rd = _node.ready();
        if (!rd.containsUpdates())
        {
            // logInfo("----- read not update");
            return;
        }
        //logInfo("------ready ------ ",_ID);
        _wal.Save(rd.hs, rd.Entries);
        if (!IsEmptySnap(rd.snap))
        {
            saveSnap(rd.snap);
            _storage.ApplySnapshot(rd.snap);
            publishSnapshot(rd.snap);
        }
        _storage.Append(rd.Entries);
        // logInfo("------ready ------ ",_ID);

        send(rd.Messages);
        if (!publishEntries(entriesToApply(rd.CommittedEntries)))
        {
            // _poll.stop();
            logError("----- poll stop");
            return;
        }

        //for readindex
        foreach (r; rd.ReadStates)
        {
            string res;
            bool iswatch = false;
            if (r.Index >= _appliedIndex)
            {
                RequestCommand command = unserialize!RequestCommand(cast(byte[]) r.RequestCtx);
                auto h = command.Hash in _request;
                if (h == null)
                {
                    continue;
                }
                auto param = tryGetJsonFormat(command.Params);
                logInfo("http GET param : ", param);
                bool recursive = false;
                if (param.type == JSONType.object && "recursive" in param)
                    recursive = param["recursive"].str == "true" ? true : false;
                if (param.type == JSONType.object && ("wait" in param)
                        && param["wait"].str == "true")
                {
                    ulong waitIndex = 0;
                    if ("waitIndex" in param)
                        waitIndex = to!ulong(param["waitIndex"].str);
                    auto w = Store.instance.Watch(command.Key, recursive, false, waitIndex);
                    w.setWatchId(command.Hash);
                    _watchers ~= w;
                    iswatch = true;
                }
                else
                {
                    auto e = Store.instance.Get(command.Key, recursive, false);
                    //value = e.getNodeValue();
                    res = makeJsonString(e);
                }
                if (!iswatch)
                {
                    h.do_response(res);
                    h.close();
                    _request.remove(command.Hash);
                }
            }
        }

        maybeTriggerSnapshot();
        _node.Advance(rd);

        // if(_node.isLeader())
        // {
        // 	if(leader() != _lastLeader)
        // 	{
        // 		logWarning("-----*****start health check *****-----");
        // 		_lastLeader = leader();
        // 		starHealthCheck();
        // 		loadServices(SERVICE_PREFIX[0..$-1]);
        // 	}
        // }
        // else
        // {
        // 	if(_healths.length > 0)
        // 	{
        // 		logWarning("-----*****stop health check *****-----");
        // 		synchronized(_mutex)
        // 		{
        // 			if(_healthPoll !is null)
        // 			{
        // 				_healthPoll.stop();
        // 				foreach(key;_healths.keys)
        // 				{
        // 					_healthPoll.delTimer(_healths[key].timerFd);
        // 				}
        // 			}
        // 			_healths.clear;
        // 			_healthPoll = null;
        // 		}
        // 	}
        // }
    }

    void scanWatchers(Object sender)
    {
        //if(_node.isLeader())
        {
            foreach (w; _watchers)
            {
                if (w.haveNotify)
                {
                    logInfo("----- scaned notify key: ", w.key, " hash :", w.watchId);
                    auto http = (w.watchId in _request);
                    if (http != null)
                    {
                        auto es = w.events();
                        foreach (e; es)
                        {
                            auto res = makeJsonString(e);
                            //logInfo("----- response msg : ",res);
                            http.do_response(res);
                            http.close();
                            break;
                        }
                        _request.remove(w.watchId);
                    }
                    removeWatcher(w.watchId);
                }
            }
        }
    }

    void handleHttpClose(size_t hash)
    {
        auto http = (hash in _request);
        if (http != null)
            _request.remove(hash);
        removeWatcher(hash);
    }

    void removeWatcher(size_t hash)
    {
        foreach (w; _watchers)
        {
            if (w.watchId == hash)
                w.Remove();
        }
        auto wl = remove!(a => a.watchId == hash)(_watchers);
        move(wl, _watchers);
        logInfo("---watchers len : ", _watchers.length);
    }

    static NetonHttpServer instance()
    {
        if (_gserver is null)
            _gserver = new NetonHttpServer();
        return _gserver;
    }

    ulong leader()
    {
        return _node._raft._lead;
    }

    void saveHttp(HttpBase h)
    {
        _request[h.toHash] = h;
    }

    // void starHealthCheck()
    // {
    // 	_healthPoll = new Epoll(100);
    // 	_healthPoll.start();
    // }

    // void addHealthCheck(string key,ref JSONValue value)
    // {
    // 	if(!_node.isLeader)
    // 		return;
    // 	if(_healthPoll !is null)
    // 	{
    // 		 synchronized(_mutex)
    // 		 {
    // 			 auto health = new Health(key,value);
    // 			if(key in _healths)
    // 			{
    // 				auto oldHlt = _healths[key];
    // 				_healthPoll.delTimer(oldHlt.timerFd);
    // 			}
    // 			_healths[key] = health;
    // 			_healthPoll.addTimer(&health.onTimer,health.interval_ms(),WheelType.WHEEL_PERIODIC);
    // 		 }
    // 	}
    // 	logInfo("-----*****health check num : ",_healths.length);
    // }

    // void removeHealthCheck(string key)
    // {
    // 	if(!_node.isLeader)
    // 		return;
    // 	synchronized(_mutex)
    // 	{
    // 		if(key in _healths)
    // 		{
    // 			auto health = _healths[key];
    // 			if(_healthPoll !is null)
    // 			{
    // 				_healthPoll.delTimer(health.timerFd);
    // 			}
    // 			_healths.remove(key);
    // 		}
    // 	}

    // 	logInfo("-----*****health check num : ",_healths.length);
    // }

    // void loadServices(string key)
    // {

    //     JSONValue node = Store.instance().getJsonValue(key);
    //     if(node.type != JSONType.null_)
    //     {
    //         auto dir = node["dir"].str == "true" ? true:false;
    //         if(!dir)
    //         {
    //             if(startsWith(key,SERVICE_PREFIX))
    // 			{
    // 				auto val = tryGetJsonFormat(node["value"].str);
    // 				addHealthCheck(key,val);
    // 			}
    //             else
    // 			{
    // 			}
    //             return ;
    //         }
    //         else
    //         {
    //             auto children = node["children"].str;
    //             if(children.length == 0)
    //             {
    //                 return ;
    //             }
    //             else
    //             {
    //                 JSONValue[] subs;
    //                 auto segments = split(children, ";");
    //                 foreach(subkey;segments)
    //                 {
    //                     if(subkey.length != 0)
    //                     {
    //                         loadServices(subkey);
    //                     }
    //                 }
    //                 return ;
    //             }

    //         }
    //     }
    //     else
    //     {
    //         return ;
    //     }
    // }

    MemoryStorage _storage;
    ulong _ID;
    // NetServer!(ServerHandler, MessageReceiver) _server;
    // NetServer!(HttpBase, NetonHttpServer) _http;
    NetServer _server;
    NetServer _http;

    NodeClient[ulong] _clients;
    RawNode _node;
    byte[] _buffer;

    bool _join;
    ulong _lastIndex;
    ConfState _confState;
    ulong _snapshotIndex;
    ulong _appliedIndex;

    HttpBase[ulong] _request;

    string _waldir; // path to WAL directory
    string _snapdir; // path to snapshot directory
    Snapshotter _snapshotter;
    WAL _wal;
    Watcher[] _watchers;

    // Poll 									_healthPoll;	 	//eventloop for health check
    Health[string] _healths;
    ulong _lastLeader = 0;
    Mutex _mutex;
}
