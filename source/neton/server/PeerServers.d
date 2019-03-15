module neton.server.PeerServers;

import core.thread;
import std.array;
import std.conv;

import hunt.logging;
import hunt.raft;
import hunt.net;

import neton.network.NodeClient;


class PeerServers
{
    private NodeClient[ulong] _clients;
	private __gshared PeerServers _gserver;
	private ulong _ID;

	static PeerServers instance()
	{
		if (_gserver is null)
			_gserver = new PeerServers();
		return _gserver;
	}

	void setID(ulong id)
	{
		_ID = id;
	}

    public bool addPeer(ulong ID, string data)
	{
		logWarning("beging do connect : ",data);
		// if (ID in _clients)
		// 	return false;

		auto client = new NodeClient(this._ID, ID);
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

	public bool delPeer(ulong ID)
	{
		if (ID !in _clients)
			return false;

		logInfo(_ID, " client disconnect ", ID);
		_clients[ID].close();
		_clients.remove(ID);

		return true;
	}

	public void send(Message[] msg)
	{
		foreach (m; msg)
			_clients[m.To].write(m);
	}
}