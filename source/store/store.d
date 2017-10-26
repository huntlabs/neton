module store.store;

import core.sync.mutex;
import store.WatcherHub;
import store.RocksdbStore;
import store.event;
import store.watcher;
import zhang2018.common.Log;
import std.json;

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

    void Init(ulong ID)
    {
        _kvStore = new RocksdbStore(ID);
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
       
        auto e  = new Event(EventAction.Get, nodePath, 0,recursive);
        e.setNetonIndex( _currentIndex);
        //e.Node.loadInternalNode(n, recursive, sorted, s.clock)

        return e;
    }

	Event Set(string nodePath, bool dir, string value )
    {
     
        // Set new value
        string error;
        auto ok= _kvStore.set(nodePath,value,error);

        _currentIndex++;
        auto e  = new Event(EventAction.Set, nodePath, _currentIndex);
        e.setNetonIndex( _currentIndex);

        //log_info("---- set event : ", e);
        if(ok)
            _watcherHub.notify(e);
        else
            e.setErrorMsg(error);
       
        //log_info("---- notify finish : ", e.nodeValue());
        return e;
    }

    Event CreateDir(string nodePath )
    {
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

    Watcher Watch(string key , bool recursive, bool stream , ulong sinceIndex)  {
        
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