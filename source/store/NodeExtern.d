module store.NodeExtern;
import std.experimental.allocator;
import std.algorithm.searching;
import store.store;
import std.json;
import hunt.logging;
import std.array;
import store.util;

class NodeExtern {

    this(string key , ulong modifiedIndex,bool recursive = false)
    {
        _recursive = recursive;
        _key = key;
        _modifiedIndex = modifiedIndex;
        _dir = false;
        loadValue();
    }

    void opAssign(S)(auto ref S ne) if(is(S == Unqual!(typeof(this))))
    {
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
        if(_key.length == 0)
            return;
        if(_recursive)
            _value = recurBuildNodeInfo(_key);
        else
        {
            JSONValue res;
            
            JSONValue node = Store.instance().getJsonValue(_key);
            if(node.type != JSON_TYPE.NULL)
            {
                res["key"] = _key;
                auto dir = node["dir"].str == "true" ? true:false;
                res["dir"] = node["dir"].str;
                if(!dir)
                {
                    if(startsWith(_key,SERVICE_PREFIX))
                        res["value"] = tryGetJsonFormat(node["value"].str);
                    else
                        res["value"] = node["value"];
                    _value = res;
                }
                else
                {
                    auto children = node["children"].str;
                    if(children.length == 0)
                    {
                        res["nodes"] = "[]";
                        _value =  res;
                    }
                    else
                    {
                        JSONValue[] subs;
                        auto segments = split(children, ";");
                        foreach(subkey;segments)
                        {
                            JSONValue sub;
                            auto j =  Store.instance().getJsonValue(subkey);
                            if(j.type != JSON_TYPE.NULL)
                            {
                                sub["key"]= subkey;
                                sub["dir"]= j["dir"].str;
                                if(j["dir"].str == "false")
                                {
                                     if(startsWith(subkey,SERVICE_PREFIX))
                                        sub["value"] = tryGetJsonFormat(j["value"].str);
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
        if(node.type == JSON_TYPE.OBJECT && "dir" in node)
            _dir = node["dir"].str == "true"?true:false;
    }

    JSONValue recurBuildNodeInfo(string key)
    {
        JSONValue res;
        res["key"] = key;
        JSONValue node = Store.instance().getJsonValue(key);
        if(node.type != JSON_TYPE.NULL)
        {
            auto dir = node["dir"].str == "true" ? true:false;
            res["dir"] = node["dir"].str;
            if(!dir)
            {
                if(startsWith(key,SERVICE_PREFIX))
                    res["value"] = tryGetJsonFormat(node["value"].str);
                else
                    res["value"] = node["value"];
                return res;
            }
            else
            {
                auto children = node["children"].str;
                if(children.length == 0)
                {
                    res["nodes"] = "[]";
                    return res;
                }
                else
                {
                    JSONValue[] subs;
                    auto segments = split(children, ";");
                    foreach(subkey;segments)
                    {
                        if(subkey.length != 0)
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
        auto ne = theAllocator.make!NodeExtern(_key,_modifiedIndex);
        ne = this ;
        return ne;
    }

    private :
            string _key;
            JSONValue _value;
            bool _dir;
            uint _expiration;
            long _ttl;
            ulong _modifiedIndex;
            bool  _recursive;
}