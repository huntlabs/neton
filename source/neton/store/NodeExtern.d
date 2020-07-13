module neton.store.NodeExtern;
import std.experimental.allocator;
import std.algorithm.searching;
import neton.store.Store;
import std.json;
import hunt.logging;
import std.array;
import neton.store.Util;

class NodeExtern
{

    this(string key, ulong modifiedIndex, bool recursive = false)
    {
        _recursive = recursive;
        _originKey = key;
        _key = getSafeKey(key);
        _modifiedIndex = modifiedIndex;
        _dir = false;
        loadValue();
    }

    this(string reqKey,string key, ulong modifiedIndex, bool recursive = false)
    {
        _recursive = recursive;
        _originKey = reqKey;
        _key = getSafeKey(key);
        _modifiedIndex = modifiedIndex;
        _dir = false;
        loadValue();
    }

    void opAssign(S)(auto ref S ne) if (is(S == Unqual!(typeof(this))))
    {
        _originKey = ne._originKey;
        _key = ne._key;
        _value = ne._value;
        _dir = ne._dir;
        _expiration = ne._expiration;
        _ttl = ne._ttl;
        _modifiedIndex = ne._modifiedIndex;
        _recursive = ne._recursive;
    }

    @property string key()
    {
        return _key;
    }

    @property string originKey()
    {
        return _originKey;
    }

    @property bool dir()
    {
        return _dir;
    }

    @property ulong modifiedIndex()
    {
        return _modifiedIndex;
    }

    void loadValue()
    {
        // logWarning("----begin load value - ----");
        // scope(exit) logWarning("----load value finish ----");
        try
        {
            if (_key.length == 0)
                return;
            if (_recursive)
                _value = recurBuildNodeInfo(_key);
            else
            {
                JSONValue res;

                JSONValue node = Store.instance().getJsonValue(_key);
                if (node.type != JSONType.null_)
                {
                    res["key"] = _originKey;
                    auto dir = node["dir"].str == "true" ? true : false;
                    res["dir"] = node["dir"].str;
                    if (!dir)
                    {
                        if (startsWith(_key, SERVICE_PREFIX))
                            res["value"] = tryGetJsonFormat(node["value"].str);
                        else if (startsWith(_key, LEASE_PREFIX[0 .. $ - 1]))
                        {
                            res["value"] = node["ttl"];
                        }
                        else
                            res["value"] = node["value"];
                        _value = res;
                    }
                    else
                    {
                        auto children = node["children"].str;
                        if (children.length == 0)
                        {
                            res["nodes"] = "[]";
                            _value = res;
                        }
                        else
                        {
                            JSONValue[] subs;
                            auto segments = split(children, ";");
                            foreach (subkey; segments)
                            {
                                JSONValue sub;
                                auto j = Store.instance().getJsonValue(subkey);
                                if (j.type != JSONType.null_)
                                {
                                    sub["key"] = j["key"].str;
                                    sub["dir"] = j["dir"].str;
                                    if (j["dir"].str == "false")
                                    {
                                        if (startsWith(subkey, SERVICE_PREFIX))
                                            sub["value"] = tryGetJsonFormat(j["value"].str);
                                        else if (startsWith(subkey, LEASE_PREFIX[0 .. $ - 1]))
                                        {
                                            sub["value"] = j["ttl"];
                                        }
                                        else
                                            sub["value"] = j["value"];
                                    }

                                    subs ~= sub;
                                }
                            }
                            res["nodes"] = subs;
                            _value = res;
                        }

                    }
                }
                else
                {
                    _value = res;
                }
            }

            auto node = _value;
            if (node.type == JSONType.object && "dir" in node)
                _dir = node["dir"].str == "true" ? true : false;

        }
        catch (Exception e)
        {
            logError("load value : ", e.msg);
        }

    }

    JSONValue recurBuildNodeInfo(string key)
    {
        JSONValue res;
        res["key"] = key;
        JSONValue node = Store.instance().getJsonValue(key);
        if (node.type != JSONType.null_)
        {
            auto dir = node["dir"].str == "true" ? true : false;
            res["dir"] = node["dir"].str;
            if (!dir)
            {
                if (startsWith(key, SERVICE_PREFIX))
                    res["value"] = tryGetJsonFormat(node["value"].str);
                else if (startsWith(key, LEASE_PREFIX[0 .. $ - 1]))
                {
                    res["value"] = node["ttl"];
                }
                else
                    res["value"] = node["value"];
                return res;
            }
            else
            {
                auto children = node["children"].str;
                if (children.length == 0)
                {
                    res["nodes"] = "[]";
                    return res;
                }
                else
                {
                    JSONValue[] subs;
                    auto segments = split(children, ";");
                    foreach (subkey; segments)
                    {
                        if (subkey.length != 0)
                        {
                            auto sub = recurBuildNodeInfo(subkey);
                            subs ~= sub;
                        }
                    }
                    res["nodes"] = subs;
                    return res;
                }

            }
        }
        else
        {
            return res;
        }
    }

    @property JSONValue value()
    {
        return _value;
    }

    NodeExtern Clone()
    {
        auto ne = theAllocator.make!NodeExtern(_originKey, _modifiedIndex);
        ne = this;
        return ne;
    }

private:
    string _key;
    string _originKey;
    JSONValue _value;
    bool _dir;
    uint _expiration;
    long _ttl;
    ulong _modifiedIndex;
    bool _recursive;
}
