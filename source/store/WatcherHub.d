module store.WatcherHub;

import store.EventHistory;
import std.algorithm.searching;
import std.range;
import std.container.dlist;
import core.sync.mutex;
import std.experimental.allocator;
import hunt.logging;
import std.string;
import store.watcher;
import store.event;
// A watcherHub contains all subscribed watchers
// watchers is a map with watched path as key and watcher as value
// EventHistory keeps the old events for watcherHub. It is used to help
// watcher to get a continuous event history. Or a watcher might miss the
// event happens between the end of the first watch command and the start
// of the second command.
alias WatcherList = DList!Watcher;
class WatcherHub {

    private :
        ulong   _count; // current number of watchers.

        Mutex   _mutex;
        WatcherList[string] _watchersMap;
        EventHistory      _eventHistory;

    public :
    // creates a watcherHub. The capacity determines how many events we will
    // keep in the eventHistory.
    // Typically, we only need to keep a small size of history[smaller than 20K].
    // Ideally, it should smaller than 20K/s[max throughput] * 2 * 50ms[RTT] = 2000
    this(int capacity)
    {
        _eventHistory = new EventHistory(capacity);
        _mutex = new Mutex;
    }

    void remove(string key, string uuid)
    {
        if((key in _watchersMap) !is null)
        {
            auto wl = _watchersMap[key];
            auto range = wl[]; 
            for ( ; !range.empty; range.popFront()) 
            { 
                auto item = range.front; 
                if (item.uuid == uuid) 
                { 
                    wl.stableLinearRemove(take(range, 1)); 
                    break; 
                } 
            } 
            if (wl[].empty) {
                    // if we have notified all watcher in the list
                    // we can delete the list
                    _watchersMap.remove(key);
                }           
        }
        logInfo("---- remove fuc call  : ", key);
    }
    // Watch function returns a Watcher.
    // If recursive is true, the first change after index under key will be sent to the event channel of the watcher.
    // If recursive is false, the first change after index at key will be sent to the event channel of the watcher.
    // If index is zero, watch will start from the current index + 1.
    Watcher watch(string key,bool recursive,bool stream, ulong index,ulong storeIndex) {
        
        auto event = _eventHistory.scan(key, recursive, index);

        auto w = theAllocator.make!Watcher(stream,recursive,index,storeIndex);

        synchronized(_mutex)
        {
             // If the event exists in the known history, append the netonIndex and return immediately
                if (event !is null) {
                    logInfo("---- find event from watcher : ", key);
                    auto ne = event.Clone();
                    ne.setNetonIndex(storeIndex);
                    w.insertEvent(ne);
                    w.setKey(key);
                    return w;
                }

                if (key in _watchersMap) { // add the new watcher to the back of the list
                    logInfo("---- key in _watchersMap : ", key);
                    auto l = _watchersMap[key];
                    l.insertBack(w);
                } else { // create a new list and add the new watcher
                    WatcherList wl;
                    wl.insertBack(w);
                    _watchersMap[key] = wl;
                    logInfo("---- new key in _watchersMap : ", wl[]);
                }

                w.setRemoveFunc(key,&this.remove);
        }

        return w;
    }

    void add(Event e) {
	    _eventHistory.addEvent(e);
    }

    // notify function accepts an event and notify to the watchers.
    void notify(Event ne) {
        auto e = _eventHistory.addEvent(ne); // add event into the eventHistory

        auto segments = split(e.nodeKey(), "/");
        logInfo("---- segments : ", segments);

        // walk through all the segments of the path and notify the watchers
        // if the path is "/foo/bar", it will notify watchers with path "/",
        // "/foo" and "/foo/bar"
        for(int i = 0; i < segments.length; i++) {
            string path ;
            for(int j =0; j <= i ;j++)
            {   
                path ~= "/" ;
                path ~= segments[j];
                // notify the watchers who interests in the changes of current path
            }
			if(path.length > 1)
				notifyWatchers(e, path[1..$], false);
            else
                notifyWatchers(e, path, false);
        }
    }

    void notifyWatchers(Event e, string nodePath , bool deleted) {
       //logInfo("---- notifyWatchers : ", nodePath);
        synchronized(_mutex)
        {
            if ((nodePath in _watchersMap) !is null) {
               // logInfo("---- in map--- : ",nodePath);
                auto wl = _watchersMap[nodePath];

                auto range = wl[]; 
                bool originalPath = (e.nodeKey() == nodePath);
                for ( ; !range.empty; range.popFront()) 
                { 
                    auto w = range.front; 
                    //logInfo("---- have watcher for--- : ",nodePath, " is originalPath :",originalPath);
                    if(/*originalPath && */ w.notify(e, originalPath, deleted))
                    {
                        if(!w.stream)
                        {
                            w.setRemoved(true);
                            wl.stableLinearRemove(take(range, 1)); 
                        }
                    }
                }

                if (wl[].empty) {
                    // if we have notified all watcher in the list
                    // we can delete the list
                    _watchersMap.remove(nodePath);
                }   
            }
        }
            
    }
}

