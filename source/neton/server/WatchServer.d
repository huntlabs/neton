module neton.server.WatchServer;

import core.thread;
import core.sync.mutex;
import std.array;
import std.conv;
import std.algorithm.mutation;

import hunt.logging;
import hunt.raft;
import hunt.net;
import hunt.event.timer.common;
import hunt.util.timer;
import hunt.event.timer;

import neton.server.NetonRpcServer;
import neton.store.watcher;
import neton.store.event;

import neton.util.Future;

import neton.v3api;
import etcdserverpb.kv;
import etcdserverpb.rpc;
import etcdserverpb.rpcrpc;
import grpc;

class WatchServer
{
	private __gshared WatchServer _gserver;
    private Watcher[] _watchers;
    private Mutex _mutex;

    private this()
    {
        _mutex = new Mutex();
    }

	static WatchServer instance()
	{
		if (_gserver is null)
			_gserver = new WatchServer();
		return _gserver;
	}

    public void run()
    {
        new Timer(NetUtil.defaultEventLoopGroup().nextLoop(), 100.msecs).onTick(
				&scanWatchers).start();
    }

    public void addWatcher(ref Watcher w)
    {
        _mutex.lock();
		scope (exit)
			_mutex.unlock();

        _watchers ~= w;
    }

    public void removeWatcher(size_t hash)
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
							auto event = new etcdserverpb.kv.Event();
							auto kv = new KeyValue();
							kv.key = cast(ubyte[])((e.nodeOriginKey()));
							kv.value = cast(ubyte[])(e.rpcValue());
							event.kv = kv;
							event.prevKv = kv;
							if (e.action() == EventAction.Delete)
								event.type = etcdserverpb.kv.Event.EventType.DELETE;
							else
								event.type = etcdserverpb.kv.Event.EventType.PUT;
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