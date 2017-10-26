module store.EventHistory;

import store.event;
import store.EventQueue;
import core.sync.mutex;
import zhang2018.common.Log;
import std.algorithm.searching;

class EventHistory {
    Mutex _mtx;
	EventQueue _queue;
	ulong      _startIndex ;
	ulong      _lastIndex ;


    this(int capacity)
    {
        _mtx = new Mutex;
        _queue = new EventQueue(capacity);
    }


    // addEvent function adds event into the eventHistory
    Event addEvent(Event e) {
        synchronized( _mtx )
        {
            _queue.insert(e);
            _lastIndex = e.Index();

            _startIndex = _queue.getIndex(_queue.front());
        }
        
        return e;
    }


    // scan enumerates events from the index history and stops at the first point
    // where the key matches.
    Event scan(string key, bool recursive,ulong index)  {
        synchronized( _mtx )
        {
             // index should be after the event history's StartIndex
            if (index < _startIndex ){
                log_error("the requested history has been cleared");
                return null;
            }

            // the index should come before the size of the queue minus the duplicate count
            if (index > _lastIndex) { // future index
                return null;
            }

            auto offset = index - _startIndex;
            auto i = (_queue.front() + cast(int)(offset)) % _queue.capacity();

            while(1) {
                auto e = _queue.event(i);

                if (!e.refresh) {
                    auto ok = (e.node().key == key);

                    if (recursive) {
                        // add tailing slash
                        auto nkey = key;
                        if (nkey[nkey.length-1] != '/') {
                            nkey = nkey ~ "/";
                        }

                        ok = ok || startsWith(e.node().key, nkey);
                    }

                    if ((e.action == EventAction.Delete || e.action == EventAction.Expire) && e.prevNode() !is null && e.prevNode().dir()) {
                        ok = ok || startsWith(key, e.prevNode().key);
                    }

                    if (ok ){
                        return e ;
                    }
                }

                i = (i + 1) % _queue.capacity;

                if (i == _queue.back) {
                    return null;
                }
            }      
        }
       // return null;
    }

}



