module server.NetonRpcServer;

import hunt.raft;
import hunt.net;
import network;
import core.time;
import core.thread;
import core.sync.mutex;
import std.string;

import store.store;
import hunt.logging;
import hunt.util.serialize;
import hunt.util.timer;
import hunt.event.timer;
import hunt.event.timer.common;
import network.client;
import server.NetonConfig;
import server.health;

import std.conv;
import wal.wal;
import snap.snapshotter;
import wal.record;
import wal.util;
import std.file;
import std.stdio;
import std.json;
import store.event;
import store.watcher;
import store.util;
import std.algorithm.mutation;

import v3api;
import etcdserverpb.kv;
import etcdserverpb.rpc;
import etcdserverpb.rpcrpc;
import grpc;

import util.Future;

enum defaultSnapCount = 10;
enum snapshotCatchUpEntriesN = 10000;

alias Event = store.event.Event;
class NetonRpcServer : MessageReceiver
{
	__gshared NetonRpcServer _gserver;

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
			node = e.nodeValue();

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
				{
					if (ents[i].Data.length == 0)
						break;

					RpcRequest command = unserialize!RpcRequest(cast(byte[]) ents[i].Data);
					logDebug("publish CMD : ", command, " ID :", _ID);
					store.event.Event resultEvent;
					auto h = (command.Hash in _request);
					iswatch = false;
					switch (command.CMD)
					{
					case RpcReqCommand.RangeRequest:
						{
							bool recursive = false;
							resultEvent = Store.instance.Get(command.Key, recursive, false);
							if (h != null)
							{
								logDebug("rpc handler do response ");

								RangeResponse respon = new RangeResponse();
								auto kv = new KeyValue();
								kv.key = cast(ubyte[])(command.Key);
								kv.value = cast(ubyte[])(resultEvent.rpcValue());
								respon.count = 1;
								respon.kvs ~= kv;
								auto handler = cast(Future!(RangeRequest, RangeResponse))(*h);
								if (handler !is null)
								{
									logDebug("handler : ", &handler);

									logDebug("response key: ", cast(string)(handler.data().key));
									handler.done(respon);
									_request.remove(command.Hash);

								}
								else
								{
									logDebug("convert rpc handler is null ");
								}
							}
							else
							{
								logDebug("rpc handler is null ");
							}
						}
						break;
					case RpcReqCommand.PutRequest:
						{
							resultEvent = Store.instance.Set(command.Key, false, command.Value);
							if (h != null)
							{
								PutResponse respon = new PutResponse();
								auto kv = new KeyValue();
								kv.key = cast(ubyte[])(command.Key);
								kv.value = cast(ubyte[])(resultEvent.rpcValue());
								respon.prevKv = kv;
								auto handler = cast(Future!(PutRequest, PutResponse))(*h);
								if (handler !is null)
								{
									// logDebug("response key: ", cast(string)(handler.data().key));
									handler.done(respon);
									_request.remove(command.Hash);

								}
								else
								{
									logDebug("convert rpc handler is null ");
								}
							}
							else
							{
								logDebug("rpc handler is null ");
							}
						}
						break;
					case RpcReqCommand.DeleteRangeRequest:
						{
							resultEvent = Store.instance.Delete(command.Key, false);
							if (h != null)
							{
								DeleteRangeResponse respon = new DeleteRangeResponse();
								auto kv = new KeyValue();
								kv.key = cast(ubyte[])(command.Key);
								kv.value = cast(ubyte[])(resultEvent.rpcValue());
								respon.prevKvs ~= kv;
								if (resultEvent.isOk)
									respon.deleted = respon.prevKvs.length;
								auto handler = cast(Future!(DeleteRangeRequest,
										DeleteRangeResponse))(*h);
								if (handler !is null)
								{
									// logDebug("response key: ", cast(string)(handler.data().key));
									handler.done(respon);
									_request.remove(command.Hash);

								}
								else
								{
									logDebug("convert rpc handler is null ");
								}
							}
							else
							{
								logDebug("rpc handler is null ");
							}
						}
						break;
					case RpcReqCommand.WatchRequest:
						{
							auto w = Store.instance.Watch(command.Key, false, false, 0);
							w.setHttpHash(command.Hash);
							_watchers ~= w;
						}
						break;
					default:
						break;
					}

					break;
				}
			case EntryType.EntryConfChange:
				{
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
				}
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

	void Propose(RpcRequest command, Object h)
	{
		logDebug("***** RpcRequest.CMD : ", command.CMD, " key : ", command.Key);

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

	void Propose(RpcRequest command)
	{
		auto err = _node.Propose(cast(string) serialize(command));
		if (err != ErrNil)
		{
			logError("---------", err);
		}
	}

	void ReadIndex(RpcRequest command, Object h)
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

		Store.instance.Init(_ID);

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

		// _http = new NetServer!(Object, NetonRpcServer)(_ID, this);

		// _http.listen("0.0.0.0", NetonConfig.instance.selfConf.apiport);

		_server = new NetServer!(Base, MessageReceiver)(_ID, this);

		_server.listen("0.0.0.0", NetonConfig.instance.selfConf.nodeport);

		foreach (peer; NetonConfig.instance.peersConf)
		{
			addPeer(peer.id, peer.ip ~ ":" ~ to!string(peer.nodeport));
		}

		new Timer(NetUtil.defaultEventLoopGroup().nextLoop(), 100.msecs).onTick(&ready).start();

		new Timer(NetUtil.defaultEventLoopGroup().nextLoop(), 100.msecs).onTick(
				&scanWatchers).start();

		new Timer(NetUtil.defaultEventLoopGroup().nextLoop(), 1000.msecs).onTick(&onTimer).start();

		NetUtil.startEventLoop(-1);

	}

	bool addPeer(ulong ID, string data)
	{
		if (ID in _clients)
			return false;

		auto client = new RaftClient(this._ID, ID);
		string[] hostport = split(data, ":");
		client.connect(hostport[0], to!int(hostport[1]), (Result!NetSocket result) {
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
		// logInfo("------ready ------ ", _ID);
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
			if (r.Index >= _appliedIndex)
			{
				RpcRequest command = unserialize!RpcRequest(cast(byte[]) r.RequestCtx);
				auto h = command.Hash in _request;
				if (h == null)
				{
					continue;
				}
				if (command.CMD == RpcReqCommand.RangeRequest)
				{
					auto e = Store.instance.Get(command.Key, false, false);

					RangeResponse respon = new RangeResponse();
					auto kv = new KeyValue();
					kv.key = cast(ubyte[])(command.Key);
					kv.value = cast(ubyte[])(e.rpcValue());
					respon.count = 1;
					respon.kvs ~= kv;
					logDebug("handler111 : ", h.toHash(), " ", _request);

					auto handler = cast(Future!(RangeRequest, RangeResponse))(*h);
					if (handler !is null)
					{
						logDebug("handler : ", h.toHash());
						// if(handler.data is null)
						// 	logDebug("##----###");
						logDebug("response key: ", cast(string)(handler.data().key));
						handler.done(respon);
						_request.remove(command.Hash);
					}
					else
					{
						logDebug("convert rpc handler is null ");
					}
				}
				else if (command.CMD == RpcReqCommand.WatchRequest)
				{
					auto w = Store.instance.Watch(command.Key, false, false, 0);
					w.setHttpHash(command.Hash);
					_watchers ~= w;
				}

			}
		}

		maybeTriggerSnapshot();
		_node.Advance(rd);

	}

	void scanWatchers(Object sender)
	{
		//if(_node.isLeader())
		{
			foreach (w; _watchers)
			{
				if (w.haveNotify)
				{
					logInfo("----- scaned notify key: ", w.key, " hash :", w.hash);
					auto h = (w.hash in _request);
					if (h != null)
					{
						auto handler = cast(Future!(ServerReaderWriter!(WatchRequest, WatchResponse), WatchResponse))(*h);
						if (handler is null)
						{
							continue;
						}
						WatchResponse respon = new WatchResponse();
						respon.created = true;
						respon.watchId = Store.instance.Index();
						auto es = w.events();
						foreach (e; es)
						{
							auto event = new etcdserverpb.kv.Event();
							auto kv = new KeyValue();
							kv.key = cast(ubyte[])(e.nodeKey());
							kv.value = cast(ubyte[])(e.rpcValue());
							event.kv = kv;
							event.prevKv = kv;
							if (e.action() == EventAction.Delete)
								event.type = etcdserverpb.kv.Event.EventType.DELETE;
							else
								event.type = etcdserverpb.kv.Event.EventType.PUT;
							respon.events ~= event;
							logInfo("--- -> notify event key: ",e.nodeKey()," value : ",e.rpcValue()," type :",e.action());
						}
						handler.data().write(respon);
						_request.remove(w.hash);
					}
					removeWatcher(w.hash);
				}
			}
		}
	}

	void removeWatcher(size_t hash)
	{
		foreach (w; _watchers)
		{
			if (w.hash == hash)
				w.Remove();
		}
		auto wl = remove!(a => a.hash == hash)(_watchers);
		move(wl, _watchers);
		logInfo("---watchers len : ", _watchers.length);
	}

	static NetonRpcServer instance()
	{
		if (_gserver is null)
			_gserver = new NetonRpcServer();
		return _gserver;
	}

	ulong leader()
	{
		return _node._raft._lead;
	}

	void saveHttp(Object h)
	{
		_request[h.toHash] = h;
	}

private:
	MemoryStorage _storage;
	ulong _ID;
	NetServer!(Base, MessageReceiver) _server;
	// NetServer!(Object, NetonRpcServer) _http;

	RaftClient[ulong] _clients;
	RawNode _node;
	byte[] _buffer;

	bool _join;
	ulong _lastIndex;
	ConfState _confState;
	ulong _snapshotIndex;
	ulong _appliedIndex;

	Object[ulong] _request;

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
