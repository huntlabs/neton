module server.NetonServer;

import protocol.Msg;
import client.client;
import client.base;
import client.http;

import raft.Raft;
import raft.Rawnode;
import raft.Storage;
import raft.Node;

import std.string;

import zhang2018.dreactor.event.Poll;
import zhang2018.dreactor.event.Epoll;
import zhang2018.dreactor.event.Select;
import zhang2018.dreactor.time.Timer;
import zhang2018.dreactor.aio.AsyncTcpServer;
import store.store;
import zhang2018.common.Log;
import zhang2018.common.Serialize;
import client.http;
import server.NetonConfig;

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
import std.algorithm.mutation;

enum defaultSnapCount = 10;
enum snapshotCatchUpEntriesN = 10000;



class NetonServer 
{
	__gshared NetonServer _gserver;

	this()
	{

	}

	void publishSnapshot(Snapshot snap)
	{
		if(IsEmptySnap(snap))
			return;

		if(snap.Metadata.Index <= _appliedIndex)
		{
			log_error(log_format("snapshot index [%d] should > progress.appliedIndex [%d] + 1", 
					snap.Metadata.Index, _appliedIndex));
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
			index: snap.Metadata.Index,
			term:  snap.Metadata.Term,
		};
		_wal.SaveSnapshot(walSnap);
		_snapshotter.SaveSnap(snap);
		
	}

	Entry[] entriesToApply(Entry[] ents)
	{
		if(ents.length == 0)
			return null;

		auto firstIdx = ents[0].Index;
		if(firstIdx > _appliedIndex + 1)
		{
			log_error(log_format("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1",
					firstIdx, _appliedIndex));
		}

		if(_appliedIndex - firstIdx + 1 < ents.length)
			return ents[_appliedIndex - firstIdx + 1 .. $];
		
		return null;
	}
    
	string makeJsonString(Event e)
	{
		if(e.error.length > 0)
			return e.error;
		JSONValue  res;
		try
		{
			JSONValue  j;
			j["action"] = e.action;
			j["netonIndex"] = Store.instance.Index();
			JSONValue  node;
			auto value = e.nodeValue();
			if(value.length != 0)
				node = parseJSON(value);

			j["node"] = node;
			return j.toString();
		}
		catch (Exception e)
		{
			log_error("catch %s", e.msg);
			res["error"] = e.msg;
		}
		return res.toString;	
	}

	bool publishEntries(Entry[] ents)
	{
		bool iswatch = false;
		for(auto i = 0 ; i < ents.length ;i++)
		{
			switch(ents[i].Type)
			{
				case EntryType.EntryNormal:
					if(ents[i].Data.length == 0)
						break;

					RequestCommand command = deserialize!RequestCommand(cast(byte[])ents[i].Data);
					
					string res;
					iswatch = false;
					switch(command.Method)
					{
						case RequestMethod.METHOD_GET:
							{
								string value;
								auto param = parseJSON(command.Params);
								log_info("http GET param : ",param);
								bool recursive = false;
								if(param.type == JSON_TYPE.OBJECT &&  "recursive" in param)
									recursive = param["recursive"].str == "true"? true:false;
								if(param.type == JSON_TYPE.OBJECT && ("wait" in param) && param["wait"].str == "true")
								{
									ulong waitIndex = 0;
									if("waitIndex" in param)
										waitIndex = to!ulong(param["waitIndex"].str);
									auto w = Store.instance.Watch(command.Key,recursive,false,waitIndex);
									w.setHttpHash(command.Hash);
									_watchers ~= w;
									iswatch = true;
								}
								else
								{
									auto e = Store.instance.Get(command.Key,recursive,false);
									value = e.nodeValue();
									res = makeJsonString(e);
								}
															
							}
							break;
						case RequestMethod.METHOD_PUT:
							{
								auto param = parseJSON(command.Params);
								log_info("http PUT param : ",param);
								bool dir = false;
								if(param.type == JSON_TYPE.OBJECT &&  "dir" in param)
										dir = param["dir"].str == "true" ? true:false;

								if(dir)
								{
									auto e = Store.instance.CreateDir(command.Key);
									res = makeJsonString(e);
								}
								else
								{
									string value ;
									if(param.type == JSON_TYPE.OBJECT &&  "value" in param)
										value = param["value"].str ;
									auto e = Store.instance.Set(command.Key ,false, value);
									res = makeJsonString(e);
								}
								
							}
							break;
						case RequestMethod.METHOD_DELETE:
							{
								auto param = parseJSON(command.Params);
								log_info("http DELETE param : ",param);
								bool recursive = false;
								if(param.type == JSON_TYPE.OBJECT &&  "recursive" in param)
									recursive = param["recursive"].str == "true"? true:false;
								auto e = Store.instance.Delete(command.Key,recursive);
								res = makeJsonString(e);
							}
							break;
						default :
							break;
					}

					//if leader
					//if(_node.isLeader())
					{
						auto http = (command.Hash in _request);
						if(http != null)
						{
							log_info("  http request params : ",http.params());
							if(!iswatch)
							{
									http.do_response(res);
									http.close();
									_request.remove(command.Hash);
							}
							
							log_info("  http request array length : ",_request.length);
						}
					}
					// else
					// {
					// 	//log_info("not leader handle http request : ",_ID);
					// }


					break;
					//next
				case EntryType.EntryConfChange:
					ConfChange cc = deserialize!ConfChange(cast(byte[])ents[i].Data);
					_confState = _node.ApplyConfChange(cc);
					switch(cc.Type)
					{
						case ConfChangeType.ConfChangeAddNode:
							if( cc.Context.length > 0)
								addPeer(cc.NodeID , cc.Context);
							break;
						case ConfChangeType.ConfChangeRemoveNode:
							if(cc.NodeID == _ID)
							{
								log_warning(_ID , " I've been removed from the cluster! Shutting down.");
								return false;
							}
							log_warning(_ID , " del node " , cc.NodeID);
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
		if(_appliedIndex - _snapshotIndex <= defaultSnapCount)
			return;

		log_info(log_format("start snapshot [applied index: %d | last snapshot index: %d]",
				_appliedIndex, _snapshotIndex));

		auto data = loadSnapshot().Data;
		Snapshot snap;
		auto err = _storage.CreateSnapshot(_appliedIndex ,&_confState , cast(string)data , snap);
		if(err != ErrNil)
		{
			log_error(err);
		}

		saveSnap(snap);

		long compactIndex = 1;
		if(_appliedIndex > snapshotCatchUpEntriesN)
			compactIndex = _appliedIndex - snapshotCatchUpEntriesN;

		_storage.Compact(compactIndex);
		log_info("compacted log at index " , compactIndex);
		_snapshotIndex = _appliedIndex;
	}


	void Propose(RequestCommand command , http h)
	{
		auto err = _node.Propose(cast(string)serialize(command));
		if( err != ErrNil)
		{
			log_error("---------",err);
		}
		else
		{
			//log_info("--------- request hash ",command.Hash);
			_request[command.Hash] = h;
		}
	}

	void ProposeConfChange(ConfChange cc)
	{
		auto err = _node.ProposeConfChange(cc);
		if( err != ErrNil)
		{
			log_error(err);
		}
	}

	Snapshot  loadSnapshot()  {
		auto snapshot = _snapshotter.loadSnap();
	
		return snapshot;
	}

	// openWAL returns a WAL ready for reading.
	void  openWAL(Snapshot snapshot) {
		if (isEmptyDir(_waldir) ){
			mkdir(_waldir);

			auto wal = new WAL(_waldir,null);
			
			if (wal is null) {
				log_error("raftexample: create wal error ", _ID);
			}
			wal.Close();
		}

		WalSnapshot walsnap;
		
		walsnap.index = snapshot.Metadata.Index;
		walsnap.term =  snapshot.Metadata.Term;
	
		log_info("loading WAL at term ", walsnap.term," and index ",walsnap.index);

		_wal = new WAL(_waldir,walsnap,true);
		
		if (_wal is null) {
			log_error("raftexample: error loading wal ", _ID);
		}
	}

	// replayWAL replays WAL entries into the raft instance.
	void replayWAL() {
		log_info("replaying WAL of member ", _ID);
		auto snapshot = loadSnapshot();
		openWAL(snapshot);

		//Snapshot *shot = null;
		HardState hs;
		Entry[] ents;
		byte[] metadata;

		_wal.ReadAll(metadata,hs,ents);

		_storage = new MemoryStorage();


		if(!IsEmptySnap(snapshot))
		{
			log_info("******* exsit snapshot : ",snapshot);
			_storage.ApplySnapshot(snapshot);
			_confState = snapshot.Metadata.CS;
			_snapshotIndex = snapshot.Metadata.Index;
			_appliedIndex = snapshot.Metadata.Index;
		}	
		
		//log_info("-------hard state : ",hs);
		//log_info("-------entry []  : ",ents);
		_storage.setHadrdState(hs);

		// append to storage so raft starts at the right place in log
		_storage.Append(ents);
		// send nil once lastIndex is published so client knows commit channel is current
		if(ents.length > 0)
		{
			_lastIndex = ents[$ - 1].Index;
		}
	}

	void start(bool join)
	{

		_ID	 			= NetonConfig.instance.selfConf.id;
		_snapdir = snapDirPath(_ID);
		_waldir = walDirPath(_ID);

		if(!Exist(_snapdir))
		{
			mkdir(_snapdir);
		}
		_snapshotter = new Snapshotter(_snapdir);

		bool oldwal = isEmptyDir(_waldir);

		replayWAL();

		Config conf = new Config();

		Store.instance.Init(_ID);

		conf._ID 				= _ID;
		conf._ElectionTick	 	= 10;
		conf._HeartbeatTick 	= 1;
		conf._storage 			= _storage;
		conf._MaxSizePerMsg		=	1024*1024;
		conf._MaxInflightMsgs	=	256;

	
		_poll 			= new Epoll();
		_buffer.length 	= 1024;

		//string[] peerstr = split(cluster , ";");
		Peer[] peers;
		Peer slf = {ID:NetonConfig.instance.selfConf.id};
		peers ~= slf;
		foreach(peer ; NetonConfig.instance.peersConf)
		{
			Peer p = {ID:peer.id};
			peers ~= p;
		}
		

		if(!oldwal)
		{
			_node = new RawNode(conf);
		}
		else
		{
			if(join)
			{
				peers.length = 0;
			}

			_node = new RawNode(conf , peers);
		}

		_http = new AsyncTcpServer!(http , byte[])(_poll , _buffer);
		_http.open("0.0.0.0" , NetonConfig.instance.selfConf.apiport);


		_server = new AsyncTcpServer!(base ,ulong, byte[])(_poll , _ID , _buffer);
		_server.open("0.0.0.0" , NetonConfig.instance.selfConf.nodeport);

		foreach(peer ; NetonConfig.instance.peersConf)
		{
			addPeer(peer.id,peer.ip ~":"~ to!string(peer.nodeport));
		}
		
		_poll.addFunc(&ready);

		_poll.addFunc(&scanWatchers);

		_poll.addTimer(&onTimer , 100 , WheelType.WHEEL_PERIODIC);

		_poll.start();

	}

	bool addPeer(ulong ID , string data)
	{
		if(ID in _clients)
			return false;

		_clients[ID] = new NodeClient(_poll , _ID , ID);
		string[] hostport = split(data , ":");
		_clients[ID].open(hostport[0] , to!ushort(hostport[1]));
		log_info(_ID , " client connect " , hostport[0] , " " , hostport[1]);
		return true;
	}

	bool delPeer(ulong ID)
	{
		if(ID !in _clients)
			return false;

		log_info(_ID , " client disconnect " , ID);
		_clients[ID].close(true);
		_clients.remove(ID);
		
		return true;
	}

	void wait()
	{
		_poll.wait();
	}

	void send(Message[] msg)
	{
		foreach(m ; msg)
			_clients[m.To].send(m);
	}

	void Step(Message msg)
	{
		_node.Step(msg);
	}

	void onTimer(TimerFd fd )
	{
		_node.Tick();
	}

	void ready()
	{
		Ready rd = _node.ready();
		if(!rd.containsUpdates())
		{
			//log_info("----- read not update");
			return;
		}
		//log_info("------ready ------ ",_ID);
		_wal.Save(rd.hs, rd.Entries);
		if( !IsEmptySnap(rd.snap))
		{
			saveSnap(rd.snap);
			_storage.ApplySnapshot(rd.snap);
			publishSnapshot(rd.snap);
		}
		_storage.Append(rd.Entries);
		send(rd.Messages);
		if(!publishEntries(entriesToApply(rd.CommittedEntries)))
		{
			_poll.stop();
			log_error("----- poll stop");
			return;
		}
		maybeTriggerSnapshot();
		_node.Advance(rd);

	}

	void scanWatchers()
	{
		//if(_node.isLeader())
		{
			foreach( w ; _watchers)
			{
				if(w.haveNotify)
				{
					log_info("----- scaned notify key: ",w.key," hash :",w.hash);
					auto http = (w.hash in _request);
					if(http != null)
					{
						auto es = w.events();
						foreach( e ; es)
						{
							auto res = makeJsonString(e);
							//log_info("----- response msg : ",res);
							http.do_response(res);
						    http.close();
							break;
						}
						_request.remove(w.hash);
					}
					removeWatcher(w.hash);
				}
			}
		}
		
	}

	void handleHttpClose(size_t hash)
	{
		auto http = (hash in _request);
		if(http != null)
			_request.remove(hash);
		removeWatcher(hash);
	}

	void removeWatcher(size_t hash)
	{
		foreach( w ; _watchers)
		{
			if(w.hash == hash)
				w.Remove();
		}
		auto wl = remove!(a => a.hash == hash)(_watchers);
		move(wl,_watchers);
		log_info("---watchers len : ",_watchers.length);
	}

	static NetonServer instance()
	{
		if(_gserver is null)
			_gserver = new NetonServer();
		return _gserver;
	}

	ulong leader()
	{
		return _node.leaderId();
	}
    
	void saveHttp(http h)
	{
		_request[h.toHash] = h;
	}

	MemoryStorage							_storage;
	Poll									_poll;
	ulong									_ID;
	AsyncTcpServer!(base,ulong ,byte[])		_server;
	AsyncTcpServer!(http , byte[])			_http;
	NodeClient[ulong]						_clients;
	RawNode									_node;
	byte[]									_buffer;


	bool									_join;
	ulong									_lastIndex;
	ConfState								_confState;
	ulong									_snapshotIndex;
	ulong									_appliedIndex;

	http[ulong]								_request;

	string 									_waldir;         // path to WAL directory
	string   							    _snapdir;        // path to snapshot directory
	Snapshotter								_snapshotter;
	WAL										_wal;
	Watcher[]								_watchers;
}

