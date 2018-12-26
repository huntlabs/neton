module server.NetonConfig;

import std.json;
import std.file;
import std.stdio;
import hunt.logging;

struct PeerConf
{
    ulong id;
    string ip;
    ushort apiport;
    ushort nodeport;
    ushort rpcport;
}

class NetonConfig
{
    __gshared NetonConfig _gconfig;

    this()
    {
    }

    void loadConf(string confPath)
    {
        try
        {
            auto data = getFileContent(confPath);
            _jconf = parseJSON(cast(string)(data));
        }
        catch (Exception e)
        {
            logError("catch netonconfig parase error : %s", e.msg);
        }
        praseConf();
    }

    byte[] getFileContent(string filename)
    {
        byte[] content;
        if (!exists(filename))
            return null;
        byte[] data = new byte[4096];
        auto ifs = File(filename, "rb");
        while (!ifs.eof())
        {
            content ~= ifs.rawRead(data);
        }
        ifs.close();
        return content;
    }

    static NetonConfig instance()
    {
        if (_gconfig is null)
            _gconfig = new NetonConfig();
        return _gconfig;
    }

    void praseConf()
    {
        if (_jconf.type == JSON_TYPE.OBJECT)
        {
            if ("self" in _jconf)
            {
                auto self = _jconf["self"].object();
                _self.id = self["id"].integer();
                _self.apiport = cast(ushort) self["apiport"].integer();
                _self.nodeport = cast(ushort) self["nodeport"].integer();
                _self.rpcport = cast(ushort) self["rpcport"].integer();
            }

            if ("peers" in _jconf)
            {
                auto peers = _jconf["peers"].array();
                foreach (peer; peers)
                {
                    PeerConf pf;
                    pf.id = peer["id"].integer();
                    pf.ip = peer["ip"].str();
                    pf.apiport = cast(ushort) peer["apiport"].integer();
                    pf.nodeport = cast(ushort) peer["nodeport"].integer();
                    pf.rpcport = cast(ushort) peer["rpcport"].integer();
                    _peersConf ~= pf;
                }
            }
        }

        logInfo("Self conf : ", _self, "  |  PeerConf : ", _peersConf);
    }

    @property PeerConf[] peersConf()
    {
        return _peersConf;
    }

    @property PeerConf selfConf()
    {
        return _self;
    }

private:
    JSONValue _jconf;
    PeerConf _self;
    PeerConf[] _peersConf;
}
