module store.store;

import core.sync.mutex;
import store.WatcherHub;
import store.RocksdbStore;
import store.event;
import store.watcher;
import hunt.logging;
import std.json;
import std.uni;
import std.algorithm.searching;
import store.util;

import etcdserverpb.rpc;
import v3api.Command;


import lease;

struct TTLOptionSet{
	uint ExpireTime ;
	bool Refresh ;
}

interface StoreInter {
	//Version() int
	ulong Index();

	Event Get(string nodePath, bool recursive, bool sorted );
	Event Set(string nodePath, bool dir, string value );
	// Event Update(string nodePath , string newValue,TTLOptionSet expireOpts );
	// Event Create(string nodePath , bool dir, string value , bool unique,
	// 	TTLOptionSet expireOpts);
	// Event CompareAndSwap(string nodePath ,string prevValue , ulong  prevIndex,
	// 	string value ,TTLOptionSet expireOpts);
	Event Delete(string nodePath,bool recursive=false);
	// Event CompareAndDelete(string nodePath ,string prevValue , ulong prevIndex);

	Watcher Watch(string prefix , bool recursive,bool stream , ulong sinceIndex ) ;

	// Save() ([]byte, error)
	// Recovery(state []byte) error

	// Clone() Store
	// SaveNoCopy() ([]byte, error)

	// JsonStats() []byte
	// DeleteExpiredKeys(cutoff time.Time)

	// HasTTLKeys() bool
}

class Store : StoreInter
{
    __gshared Store _gstore;

    void Init(ulong ID, Lessor l)
    {
        _kvStore = new RocksdbStore(ID,l);
    }

    this()
	{
		_currentIndex = 0;
        _watcherHub = new WatcherHub(1000);
	}

    // Index retrieves the current index of the store.
    ulong Index() {
        return _currentIndex;
    }

    // Get returns a get event.
    // If recursive is true, it will return all the content under the node path.
    // If sorted is true, it will sort the content by keys.
    Event Get(string nodePath, bool recursive, bool sorted )
    {
        nodePath = getSafeKey(nodePath);
        auto e  = new Event(EventAction.Get, nodePath, 0,recursive);
        e.setNetonIndex( _currentIndex);
        //e.Node.loadInternalNode(n, recursive, sorted, s.clock)

        return e;
    }

    // set value
	Event Set(string nodePath, bool dir, string value )
    {
        nodePath = getSafeKey(nodePath);
        // Set new value
        string error;
        auto ok= _kvStore.set(nodePath,value,error);

        _currentIndex++;
        auto e  = new Event(EventAction.Set, nodePath, _currentIndex);
        e.setNetonIndex( _currentIndex);

        //logInfo("---- set event : ", e);
        if(ok)
            _watcherHub.notify(e);
        else
            e.setErrorMsg(error);
       
        //logInfo("---- notify finish : ", e.nodeValue());
        return e;
    }

    // create dir
    Event CreateDir(string nodePath )
    {
        nodePath = getSafeKey(nodePath);

        string error;
        auto ok= _kvStore.createDir(nodePath,error);

        _currentIndex++;
        auto e  = new Event(EventAction.Create, nodePath, _currentIndex);
        e.setNetonIndex( _currentIndex);

        if(ok)
            _watcherHub.notify(e);
        else
            e.setErrorMsg(error);

        return e;
    }

    // watch key or dir
    Watcher Watch(string key , bool recursive, bool stream , ulong sinceIndex)  {
        key = getSafeKey(key);

        auto keys = key;
        if (sinceIndex == 0) {
            sinceIndex = _currentIndex + 1;
        }
        // WatcherHub does not know about the current index, so we need to pass it in
        auto w = _watcherHub.watch(keys, recursive, stream, sinceIndex, _currentIndex);
        if (w is null) {
            return null;
        }
        return w;
    }

    // Delete deletes the node at the given path.
    Event Delete(string nodePath,bool recursive = false) {
        nodePath = getSafeKey(nodePath);
        
        _currentIndex++;
        auto e  = new Event(EventAction.Delete, nodePath, _currentIndex);
        e.setNetonIndex( _currentIndex);

        if(e.dir && recursive == false)
        {
            e.setErrorMsg(nodePath ~" is dir , please use recursive option");
        }
        else
        {
            _watcherHub.notify(e);
       
            _kvStore.Remove(nodePath,recursive);
        }

        return e;
    }

    Event Register(ref JSONValue server)
    {
        auto service = server["service"];
        string id,name,key = SERVICE_PREFIX;
        if(service.type == JSON_TYPE.OBJECT)
        {
            id = toLower(service["id"].str);
            name = toLower(service["name"].str);
            key ~= name;
            key ~= "/";
            key ~= id;
        }
        server["status"] = ServiceState.Passing;

        string error;
        auto ok= _kvStore.set(key,server.toString,error);

        _currentIndex++;
        auto e  = new Event(EventAction.Register, key, _currentIndex);
        e.setNetonIndex( _currentIndex);

        //logInfo("---- set event : ", e);
        if(ok)
            _watcherHub.notify(e);
        else
            e.setErrorMsg(error);
       
        //logInfo("---- notify finish : ", e.nodeValue());
        return e;
    }

    Event Deregister(ref JSONValue server)
    {
        string id,name,key = SERVICE_PREFIX;
        if(server.type == JSON_TYPE.OBJECT)
        {
            id = toLower(server["id"].str);
            name = toLower(server["name"].str);
            key ~= name;
            key ~= "/";
            key ~= id;
        }

        auto e  = new Event(EventAction.Deregister, key, _currentIndex+1);
        e.setNetonIndex( _currentIndex+1);

        auto value = getStringValue(key);
        if(value.length > 0)
        {
            _currentIndex++;
            _kvStore.Remove(key,false);
            _watcherHub.notify(e);
        }
        else
        {
            e.setNetonIndex( _currentIndex);
            e.setErrorMsg("service not found!");
        }
 
        return e;
    }

    /// rpc 

    PutResponse put(RpcRequest req)
    {
        auto respon = _kvStore.put(req);

        if(respon !is null)
        {
            _currentIndex++;
            auto e  = new Event(EventAction.Set, getSafeKey(req.Key), _currentIndex);
            e.setNetonIndex( _currentIndex);

            _watcherHub.notify(e);
        }
        return respon;
    }

    long generateLeaseID()
    {
        return _kvStore.generateLeaseID();
    }

	Lease grantLease(long leaseid, long ttl)
    {
        return _kvStore.grantLease(leaseid,ttl);
    }

	bool revokeLease(long leaseid)
    {
        return _kvStore.revokeLease(leaseid);
    }

    LeaseTimeToLiveResponse leaseTimeToLive(long leaseid)
    {
        return _kvStore.leaseTimeToLive(leaseid);
    }

	LeaseLeasesResponse leaseLeases()
    {
        return _kvStore.leaseLeases();
    }

    LeaseKeepAliveResponse renewLease(RpcRequest req)
    {
        return _kvStore.renewLease(req);
    }
    


    static Store instance()
	{
		if(_gstore is null)
			_gstore = new Store();
		return _gstore;
	}

    string getStringValue(string key)
    {
        return _kvStore.getStringValue(key);
    }

    JSONValue getJsonValue(string key)
    {
        return _kvStore.getJsonValue(key);
    }
    // void setKeyValue(string key , string value)
    // {
    //     _kvStore.SetValue(key,value);
    // }
    private :
            RocksdbStore _kvStore;
            WatcherHub _watcherHub;
            ulong      _currentIndex;
            Mutex      _mtx;

}