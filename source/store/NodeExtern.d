module store.NodeExtern;
import std.experimental.allocator;
import store.store;
import std.json;
import zhang2018.common.Log;
import std.array;

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
                    res["value"] = node["value"].str;
                    _value = res.toString;
                }
                else
                {
                    auto children = node["children"].str;
                    if(children.length == 0)
                    {
                        res["nodes"] = "[]";
                        _value =  res.toString;
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
                                    sub["value"] = j["value"].str;
                                subs ~= sub;
                            }
                        }
                        res["nodes"] = subs;
                        _value = res.toString;
                    }
                    
                }
            }
            else
            {
                _value = res.toString;
            }
        }

        auto node = parseJSON(_value);
        if(node.type == JSON_TYPE.OBJECT && "dir" in node)
            _dir = node["dir"].str == "true"?true:false;
    }

    string recurBuildNodeInfo(string key)
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
                res["value"] = node["value"].str;
                return res.toString;
            }
            else
            {
                auto children = node["children"].str;
                if(children.length == 0)
                {
                    res["nodes"] = "[]";
                    return res.toString;
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
                            subs ~= parseJSON(sub);
                        }
                    }
                    res["nodes"] = subs;
                    return res.toString;
                }
                
            }
        }
        else
        {
            return res.toString;
        }
    }

    @property string value()
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
            string _value;
            bool _dir;
            uint _expiration;
            long _ttl;
            ulong _modifiedIndex;
            bool  _recursive;
}