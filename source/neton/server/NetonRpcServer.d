module neton.server.NetonRpcServer;

import hunt.raft;
import hunt.net;
import neton.network.NetServer;
import neton.network.NodeClient;
import neton.network.ServerHandler;
import neton.network.Interface;

import core.time;
import core.thread;
import core.sync.mutex;
import std.string;

import neton.store.Store;
import hunt.logging;
import hunt.util.Serialize;
import hunt.util.Timer;
import hunt.event.timer;
import hunt.event.timer.Common;
import neton.server.NetonConfig;
import neton.server.PeerServers;
import neton.server.WatchServer;

import std.conv;
import neton.wal.Wal;
import neton.snap.SnapShotter;
import neton.wal.Record;
import neton.wal.Util;
import std.file;
import std.stdio;
import std.json;
import neton.store.Event;
import neton.store.Watcher;
import neton.store.Util;
import std.algorithm.mutation;

import neton.rpcservice;
import etcdserverpb.kv;
import etcdserverpb.rpc;
import etcdserverpb.rpcrpc;
import grpc;

import neton.util.Future;
import neton.util.Queue;

import neton.lease;

enum defaultSnapCount = 10;
enum snapshotCatchUpEntriesN = 10000;

alias Event = neton.store.Event.Event;
class NetonRpcServer : MessageReceiver
{
	private
	{
		__gshared NetonRpcServer _gserver;

		MemoryStorage _storage;
		ulong _ID;
		NetServer!(ServerHandler, MessageReceiver) _server;

		RawNode _node;

		bool _join;
		ulong _lastIndex;
		ConfState _confState;
		ulong _snapshotIndex;
		ulong _appliedIndex;

		Object[ulong] _request; /// key is hashId

		string _waldir; // path to WAL directory
		string _snapdir; // path to snapshot directory
		Snapshotter _snapshotter;
		WAL _wal;

		ulong _lastLeader = 0;
		Mutex _mutex;

		Lessor _lessor;
		Queue!RpcRequest _proposeQueue;
	}

public:

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

		LessorConfig lessorConf;
		lessorConf.MinLeaseTTL = 1;
		_lessor = NewLessor(lessorConf);

		Store.instance.Init(_ID, _lessor);

		conf._ID = _ID;
		conf._ElectionTick = 10;
		conf._HeartbeatTick = 1;
		conf._storage = _storage;
		conf._MaxSizePerMsg = 1024 * 1024;
		conf._MaxInflightMsgs = 256;

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

		_server = new NetServer!(ServerHandler, MessageReceiver)(_ID, this);
		_server.listen("0.0.0.0", NetonConfig.instance.selfConf.nodeport);

		PeerServers.instance().setID(_ID);
		foreach (peer; NetonConfig.instance.peersConf)
		{
			PeerServers.instance().addPeer(peer.id, peer.ip ~ ":" ~ to!string(peer.nodeport));
		}

		new Timer(NetUtil.defaultEventLoopGroup().nextLoop(), 100.msecs).onTick(&ready).start();

		WatchServer.instance().run();

		new Timer(NetUtil.defaultEventLoopGroup().nextLoop(), 1.seconds).onTick(&onTimer).start();

		new Timer(NetUtil.defaultEventLoopGroup().nextLoop(), 200.msecs).onTick(&onLessor).start();

		new Timer(NetUtil.defaultEventLoopGroup().nextLoop(), 100.msecs).onTick(&onPropose)
			.start();

		NetUtil.startEventLoop(-1);

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
					neton.store.Event.Event resultEvent;
					auto h = (command.Hash in _request);
					iswatch = false;
					switch (command.CMD)
					{
					case RpcReqCommand.RangeRequest:
					case RpcReqCommand.ConfigRangeRequest:
					case RpcReqCommand.RegistryRangeRequest:
						{
							auto respon = Store.instance.get(command);
							if (h != null)
							{
								auto handler = cast(Future!(RangeRequest, RangeResponse))(*h);
								if (handler !is null)
								{
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
								// logDebug("rpc handler is null ");
							}
						}
						break;
					case RpcReqCommand.PutRequest:
					case RpcReqCommand.ConfigPutRequest:
					case RpcReqCommand.RegistryPutRequest:
						{
							auto respon = Store.instance.put(command);
							if (h != null)
							{
								auto handler = cast(Future!(PutRequest, PutResponse))(*h);
								if (handler !is null)
								{
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
								// logDebug("rpc handler is null ");
							}
						}
						break;
					case RpcReqCommand.DeleteRangeRequest:
					case RpcReqCommand.ConfigDeleteRangeRequest:
					case RpcReqCommand.RegistryDeleteRangeRequest:
						{
							auto respon = Store.instance.deleteRange(command);
							if (h != null)
							{
								auto handler = cast(Future!(DeleteRangeRequest,
										DeleteRangeResponse))(*h);
								if (handler !is null)
								{
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
								// logDebug("rpc handler is null ");
							}
						}
						break;
					case RpcReqCommand.WatchRequest:
						{
							auto w = Store.instance.Watch(command.Key, false, true, 0);
							w.setHash(command.Hash);
							w.setWatchId(command.Value.to!long);
							WatchServer.instance().addWatcher(w);
						}
						break;
					case RpcReqCommand.LeaseGenIDRequest:
						{
							if (_node.isLeader) /// leader gen leaseID
							{
								long leaseID = Store.instance.generateLeaseID();
								command.LeaseID = leaseID;
								command.CMD = RpcReqCommand.LeaseGrantRequest;
								Propose(command);
							}
						}
						break;
					case RpcReqCommand.LeaseGrantRequest:
						{
							Lease l = Store.instance.grantLease(command.LeaseID, command.TTL);
							if (h != null)
							{
								LeaseGrantResponse respon = new LeaseGrantResponse();
								respon.ID = command.LeaseID;
								if (l is null)
									respon.error = "grant fail";
								else
									respon.TTL = l.ttl;

								auto handler = cast(Future!(LeaseGrantRequest, LeaseGrantResponse))(
										*h);
								if (handler !is null)
								{
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
								// logDebug("rpc handler is null ");
							}
						}
						break;
					case RpcReqCommand.LeaseRevokeRequest:
						{
							auto ok = Store.instance.revokeLease(command.LeaseID);
							if (!ok)
							{
								logWarning("revoke lease fail : ", command.LeaseID);
							}
							if (h != null)
							{
								LeaseRevokeResponse respon = new LeaseRevokeResponse();

								auto handler = cast(Future!(LeaseRevokeRequest,
										LeaseRevokeResponse))(*h);
								if (handler !is null)
								{
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
								// logDebug("rpc handler is null ");
							}
						}
						break;
					case RpcReqCommand.LeaseTimeToLiveRequest:
						{
							auto respon = Store.instance.leaseTimeToLive(command.LeaseID);
							if (respon is null)
							{
								logWarning("LeaseTimeToLiveRequest fail : ", command.LeaseID);
							}
							if (h != null)
							{
								auto handler = cast(Future!(LeaseTimeToLiveRequest,
										LeaseTimeToLiveResponse))(*h);
								if (handler !is null)
								{
									logDebug("LeaseTimeToLiveRequest reponse ID:", respon.ID, "  ttl :",
											respon.TTL, " grantedTTL:", respon.grantedTTL);
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
								// logDebug("rpc handler is null ");
							}
						}
						break;
					case RpcReqCommand.LeaseLeasesRequest:
						{
							auto respon = Store.instance.leaseLeases();
							if (respon is null)
							{
								logWarning("LeaseLeasesRequest fail ");
							}
							if (h != null)
							{
								auto handler = cast(Future!(LeaseLeasesRequest,
										LeaseLeasesResponse))(*h);
								if (handler !is null)
								{
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
								// logDebug("rpc handler is null ");
							}
						}
						break;
					case RpcReqCommand.LeaseKeepAliveRequest:
						{
							auto respon = Store.instance.renewLease(command);
							if (respon is null)
							{
								logWarning("LeaseKeepAliveRequest fail : ", _ID);
							}
							if (h != null)
							{
								auto handler = cast(Future!(ServerReaderWriter!(LeaseKeepAliveRequest,
										LeaseKeepAliveResponse), LeaseKeepAliveResponse))(*h);
								if (handler !is null)
								{
									handler.data().write(respon);
									_request.remove(command.Hash);
								}
								else
								{
									logDebug("convert rpc handler is null ");
								}
							}
							else
							{
								// logDebug("rpc handler is null ");
							}
						}
						break;
					case RpcReqCommand.WatchCancelRequest:
						{
							WatchServer.instance().removeWatcher(command.Value.to!long);
							if (h != null)
							{
								auto handler = cast(Future!(ServerReaderWriter!(WatchRequest,
										WatchResponse), WatchInfo))(*h);
								if (handler !is null)
								{
									WatchResponse respon = new WatchResponse();
									WatchInfo watchInfo = handler.ExtraData();
									respon.header = watchInfo.header;
									respon.created = false;
									respon.canceled = true;
									respon.watchId = watchInfo.watchId;
									logDebug("watch cancel --------------: ",
											respon.watchId, " revision :", respon.header.revision);
									handler.data().write(respon);
									_request.remove(command.Hash);
								}
								else
								{
									logDebug("convert rpc handler is null ");
								}
							}
							else
							{
								// logDebug("rpc handler is null ");
							}
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
							PeerServers.instance().addPeer(cc.NodeID, cc.Context);
						break;
					case ConfChangeType.ConfChangeRemoveNode:
						if (cc.NodeID == _ID)
						{
							logWarning(_ID, " I've been removed from the cluster! Shutting down.");
							return false;
						}
						logWarning(_ID, " del node ", cc.NodeID);
						PeerServers.instance().delPeer(cc.NodeID);
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

		PeerServers.instance().send(rd.Messages);
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
				if (command.CMD == RpcReqCommand.RangeRequest 
					|| command.CMD == RpcReqCommand.ConfigRangeRequest
					|| command.CMD == RpcReqCommand.RegistryRangeRequest)
				{
					auto respon = Store.instance.get(command);

					foreach (kv; respon.kvs)
						logDebug("KeyValue pair (%s , %s)".format(cast(string)(kv.key),
								cast(string)(kv.value)));
					logDebug("handler keyValue len : ", respon.count);

					auto handler = cast(Future!(RangeRequest, RangeResponse))(*h);
					if (handler !is null)
					{
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
					auto w = Store.instance.Watch(command.Key, false, true, 0);
					w.setHash(command.Hash);
					w.setWatchId(command.Value.to!long);
					WatchServer.instance().addWatcher(w);
				}

			}
		}

		maybeTriggerSnapshot();
		_node.Advance(rd);

		if (leader() != _lastLeader)
		{
			_lastLeader = leader();
			if (_node.isLeader())
			{
				if (_lessor !is null)
					_lessor.Promote(1);
			}
			else
				_lessor.Demote();
		}

	}

	void onTimer(Object sender)
	{
		_mutex.lock();
		scope (exit)
			_mutex.unlock();

		_node.Tick();
	}

	void onPropose(Object)
	{
		auto req = _proposeQueue.pop();
		if (req != req.init)
		{
			_mutex.lock();
			scope (exit)
				_mutex.unlock();
			auto err = _node.Propose(cast(string) serialize(req));
			if (err != ErrNil)
			{
				logError("---------", err);
			}
		}
	}

	void onLessor(Object sender)
	{
		_mutex.lock();
		scope (exit)
			_mutex.unlock();

		_lessor.runLoop();
	}

	void Propose(RpcRequest command, Object h)
	{
		logDebug("***** RpcRequest.CMD : ", command.CMD, " key : ", command.Key);
		if (command.CMD == RpcReqCommand.WatchCancelRequest)
		{
			if (command.Hash !in _request)
				_request[command.Hash] = h;
		}
		else
			_request[command.Hash] = h;
		Propose(command);
	}

	void Propose(RpcRequest command)
	{
		_proposeQueue.push(command);
	}

	void ReadIndex(RpcRequest command, Object h)
	{
		_mutex.lock();
		scope (exit)
			_mutex.unlock();

		_node.ReadIndex(cast(string) serialize(command));
		_request[command.Hash] = h;
	}

	void step(Message msg)
	{
		_mutex.lock();
		scope (exit)
			_mutex.unlock();

		_node.Step(msg);
	}

	Object getRequest(size_t hash)
	{
		auto obj = (hash in _request);
		if (obj != null)
		{
			return *obj;
		}
		else
			return null;
	}

	ulong leader()
	{
		_mutex.lock();
		scope (exit)
			_mutex.unlock();
		return _node._raft._lead;
	}

	static NetonRpcServer instance()
	{
		if (_gserver is null)
			_gserver = new NetonRpcServer();
		return _gserver;
	}

private:

	this()
	{
		_proposeQueue = new Queue!RpcRequest();
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

	void ProposeConfChange(ConfChange cc)
	{
		_mutex.lock();
		scope (exit)
			_mutex.unlock();

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

	void maybeTriggerSnapshot()
	{
		if (_appliedIndex - _snapshotIndex <= defaultSnapCount)
			return;

		logInfof("start snapshot [applied index: %d | last snapshot index: %d]",
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

}
