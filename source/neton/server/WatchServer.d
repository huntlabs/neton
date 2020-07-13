module neton.server.WatchServer;

import core.thread;
import core.sync.mutex;
import std.array;
import std.conv;
import std.algorithm.mutation;

import hunt.logging;
import hunt.raft;
import hunt.net;
import hunt.event.timer.Common;
import hunt.util.Timer;
import hunt.event.EventLoop;
import hunt.event.timer;

import neton.server.NetonRpcServer;
import neton.store.Watcher;
import neton.store.Event;

import neton.util.Future;

import neton.rpcservice;
import neton.protocol.neton;
import neton.protocol.neton;
import neton.protocol.netonrpc;
import grpc;

class WatchServer
{
	private __gshared WatchServer _gserver;
    private Watcher[] _watchers;
    private Mutex _mutex;
	private EventLoop _eventLoop;
	private ITimer _timer;

    private this()
    {
        _mutex = new Mutex();
		_eventLoop = new EventLoop();
        _timer = new Timer(_eventLoop, 100.msecs).onTick(&scanWatchers);
		_eventLoop.run(-1);
    }

	static WatchServer instance()
	{
		if (_gserver is null)
			_gserver = new WatchServer();
		return _gserver;
	}

    void run()
    {
        _timer.start();
    }

    void addWatcher(ref Watcher w)
    {
        _mutex.lock();
		scope (exit)
			_mutex.unlock();

        _watchers ~= w;
    }

    void removeWatcher(size_t hash)
	{
        _mutex.lock();
		scope (exit)
			_mutex.unlock();

		foreach (w; _watchers)
		{
			if (w.watchId == hash)
				w.Remove();
		}
		auto wl = remove!(a => a.watchId == hash)(_watchers);
		move(wl, _watchers);
		logInfo("---watchers len : ", _watchers.length);
	}

    void scanWatchers(Object sender)
	{
		_mutex.lock();
		scope (exit)
			_mutex.unlock();
		{
			foreach (w; _watchers)
			{
				if (w.haveNotify)
				{
					logInfo("----- scaned notify key: ", w.key, " hash :", w.hash);
					auto h = NetonRpcServer.instance().getRequest(w.hash);
					if (h !is null)
					{
						auto handler = cast(Future!(ServerReaderWriter!(WatchRequest,
								WatchResponse), WatchInfo))(h);
						if (handler is null)
						{
							logWarning("--- watch handler convert fail ---");
							continue;
						}
						WatchResponse respon = new WatchResponse();
						WatchInfo watchInfo = handler.ExtraData();
						respon.header = watchInfo.header;
						respon.created = false;
						respon.watchId = watchInfo.watchId;

						handler.ExtraData().header.revision +=1;

						auto es = w.events();
						foreach (e; es)
						{
							auto event = new neton.protocol.neton.Event();
							auto kv = new KeyValue();
							kv.key = cast(ubyte[])((e.nodeOriginKey()));
							kv.value = cast(ubyte[])(e.rpcValue());
							event.kv = kv;
							event.prevKv = kv;
							if (e.action() == EventAction.Delete)
								event.type = neton.protocol.neton.Event.EventType.DELETE;
							else
								event.type = neton.protocol.neton.Event.EventType.PUT;
							respon.events ~= event;
							logInfo("--- -> notify event key: ", e.nodeOriginKey(),
									" value : ", e.rpcValue(), " type :", e.action());
						}
						w.clearEvent();
						handler.data().write(respon);
					}
				}
			}
		}
	}
}