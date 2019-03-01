module neton.store.Watcher;

import neton.store.Event;
import std.uuid;
import hunt.logging;

interface WatcherInter {
	Event[] events();
	ulong StartIndex();  // The netonIndex at which the Watcher was created
	void Remove();
}

alias RemoveFunc = void delegate(string,string);

const int MAX_EVENTS = 100;

class Watcher :  WatcherInter {

    this()
    {
        _uuid = randomUUID().toString;
    }

    this(bool stream,bool recursive,ulong sinceIndex,ulong startIndex)
    {
        _uuid = randomUUID().toString;
        _stream = stream;
        _recursive = recursive;
        _sinceIndex = sinceIndex;
        _startIndex = startIndex;
    }

    Event[] events()
    {
        return _events;
    }

    void clearEvent()
    {
        _events.length = 0;
    }

    void insertEvent(Event e)
    {
        _events ~= e;
    }

    @property bool haveNotify()
    {
        return _events.length != 0;
    }

    @property string uuid()
    {
        return _uuid;
    }

    @property ulong StartIndex()
    {
        return _startIndex;
    }

    @property bool  stream()
    {
        return _stream;
    }

    @property bool  removed()
    {
        return _removed;
    }

    void setRemoved(bool f)
    {
        _removed = f;
    }

    void setKey(string key)
    {
        _key = key;
    }

    @property string key()
    {
        return _key;
    }

    void setWatchId(size_t wid)
    {
        _watchId = wid;
    }

    @property size_t watchId()
    {
        return _watchId;
    }

    void setHash(size_t hash)
    {
        _hash = hash;
    }

    @property size_t hash()
    {
        return _hash;
    }

    // notify function notifies the watcher. If the watcher interests in the given path,
    // the function will return true.
    bool notify(Event e, bool originalPath, bool deleted)  {
        // watcher is interested the path in three cases and under one condition
        // the condition is that the event happens after the watcher's sinceIndex

        // 1. the path at which the event happens is the path the watcher is watching at.
        // For example if the watcher is watching at "/foo" and the event happens at "/foo",
        // the watcher must be interested in that event.

        // 2. the watcher is a recursive watcher, it interests in the event happens after
        // its watching path. For example if watcher A watches at "/foo" and it is a recursive
        // one, it will interest in the event happens at "/foo/bar".

        // 3. when we delete a directory, we need to force notify all the watchers who watches
        // at the file we need to delete.
        // For example a watcher is watching at "/foo/bar". And we deletes "/foo". The watcher
        // should get notified even if "/foo" is not the path it is watching.
        //logInfo("---- have a notify2 : ", _recursive," e.index :", e.Index()," _sinceIndex : ",_sinceIndex );
        if ((_recursive || originalPath || deleted) && e.Index() >= _sinceIndex) {
            // We cannot block here if the event array capacity is full, otherwise
            // neton will hang. event array capacity is full when the rate of
            // notifications are higher than our send rate.
            // If this happens, we close the channel.
            
            logInfo("---- have a notify : ", _key);
            if(_events.length >= MAX_EVENTS)
            {
                logWarning("---- too many events  : ", _key);
                Remove();
                return true;
            }
            _events ~= e;
            return true;
        }
        return false;
    }


    // Remove removes the watcher from watcherHub
    // The actual remove function is guaranteed to only be executed once
    void Remove() {
        _events.length = 0;
        if (_remove !is null ){
            _removed = true;
            _remove(_key,_uuid);
        }
    }

    void setRemoveFunc(string key,RemoveFunc func)
    {
        _key = key;
        _remove = func;
    }

    private : 
        	Event[]   _events;
            bool       _stream;
            bool       _recursive;
            ulong      _sinceIndex;
            ulong      _startIndex;
            
            bool       _removed;
            RemoveFunc _remove;
            string     _uuid;
            string     _key;
            size_t     _watchId;
            size_t     _hash;
}

