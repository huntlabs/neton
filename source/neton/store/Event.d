module neton.store.Event;

import neton.store.NodeExtern;
import std.experimental.allocator;
import std.json;
import hunt.logging;
import neton.store.Util;

enum ServiceState
{
    Passing = "passing",
    Warning = "warning",
    Critical = "critical",
    Maintenance = "maintenance",
}

enum EventAction
{
    Get = "get",
    Create = "create",
    Set = "set",
    Update = "update",
    Delete = "delete",
    Register = "register",
    Deregister = "deregister",
    CompareAndSwap = "compareAndSwap",
    CompareAndDelete = "compareAndDelete",
    Expire = "expire",
}

class Event
{

    this()
    {

    }

    this(EventAction action, string key, ulong modifiedIndex, bool recursive = false)
    {

        // logWarning("begin load value :",key );
        _node = new NodeExtern(key, modifiedIndex, recursive);
        // logWarning("end load value :",key );
        _action = action;
        _refresh = false;
    }

    this(EventAction action, string reqKey,string key, ulong modifiedIndex, bool recursive = false)
    {

        // logWarning("begin load value :",key );
        _node = new NodeExtern(reqKey,key, modifiedIndex, recursive);
        // logWarning("end load value :",key );
        _action = action;
        _refresh = false;
    }

    void opAssign(S)(auto ref S e) if (is(S == Unqual!(typeof(this))))
    {
        _action = e._action;
        _netonIndex = e._netonIndex;
        _refresh = e._refresh;
        _node = e._node.Clone();
        _prevNode = e._prevNode.Clone();
        _errorMsg = e._errorMsg;
    }

    bool IsCreated()
    {
        if (this._action == EventAction.Create)
        {
            return true;
        }
        return _action == EventAction.Set && _prevNode is null;
    }

    @property ulong Index()
    {
        return  /*_node.modifiedIndex*/ _netonIndex;
    }

    @property EventAction action()
    {
        return _action;
    }

    @property NodeExtern prevNode()
    {
        return _prevNode;
    }

    @property NodeExtern node()
    {
        return _node;
    }

    string nodeKey()
    {
        return _node.key;
    }

    string nodeOriginKey()
    {
        return _node.originKey;
    }

    JSONValue getNodeValue()
    {
        return _node.value;
    }

    @property ulong netonIndex()
    {
        return _netonIndex;
    }

    void setNetonIndex(ulong idx)
    {
        _netonIndex = idx;
    }

    Event Clone()
    {
        auto e = theAllocator.make!Event();
        e = this;
        return e;
    }

    @property bool dir()
    {
        return _node.dir;
    }

    @property bool refresh()
    {
        return _refresh;
    }

    void SetRefresh()
    {
        _refresh = true;
    }

    @property bool isOk()
    {
        return _errorMsg.length == 0;
    }

    @property string error()
    {
        return _errorMsg;
    }

    void setErrorMsg(string msg)
    {
        _errorMsg = msg;
    }

    string rpcValue()
    {
        if (this.error.length > 0)
            return this.error;
        JSONValue nodeValue = this.getNodeValue();
        if (nodeValue.type == JSONType.OBJECT)
        {
            if ("value" in nodeValue)
            {
                if (nodeValue["value"].type == JSONType.STRING)
                    return nodeValue["value"].str;
                else
                    return nodeValue["value"].toString;
            }
        }
        return nodeValue.toString;
    }

    import neton.protocol.neton;

    KeyValue[] getKeyValues()
    {
        KeyValue[] kvs;
        if (this.error.length > 0)
            return null;
        JSONValue nodeValue = this.getNodeValue();
        if(nodeValue.type == JSONType.NULL)
            return null;
        if (isDir(nodeValue))
        {
            nodeValue = nodeValue["nodes"];
            if (nodeValue.type == JSONType.OBJECT)
            {
                if (!isDir(nodeValue))
                {
                    auto kv = new KeyValue();
                    kv.key = cast(ubyte[])(nodeValue["key"].str);
                    if ("value" in nodeValue)
                    {
                        if (nodeValue["value"].type == JSONType.STRING)
                            kv.value = cast(ubyte[])(nodeValue["value"].str);
                        else
                            kv.value = cast(ubyte[])(nodeValue["value"].toString);
                    }
                    kvs ~= kv;
                }
            }
            else if (nodeValue.type == JSONType.ARRAY)
            {
                foreach (item; nodeValue.array)
                {
                    if (!isDir(item))
                    {
                        auto kv = new KeyValue();
                        kv.key = cast(ubyte[])(item["key"].str);
                        if ("value" in item)
                        {
                            if (item["value"].type == JSONType.STRING)
                                kv.value = cast(ubyte[])(item["value"].str);
                            else
                                kv.value = cast(ubyte[])(item["value"].toString);
                        }
                        kvs ~= kv;
                    }
                }
            }
        }
        else
        {
            auto kv = new KeyValue();
            kv.key = cast(ubyte[])(nodeValue["key"].str);
            if ("value" in nodeValue)
            {
                if (nodeValue["value"].type == JSONType.STRING)
                    kv.value = cast(ubyte[])(nodeValue["value"].str);
                else
                    kv.value = cast(ubyte[])(nodeValue["value"].toString);
            }
            kvs ~= kv;
        }

        return kvs;
    }

    bool isDir(JSONValue data)
    {
        if (data.type != JSONType.OBJECT)
            return false;
        if ("dir" in data && data["dir"].str == "true")
            return true;
        return false;
    }

private:
    EventAction _action;
    NodeExtern _node;
    NodeExtern _prevNode;
    ulong _netonIndex;
    bool _refresh;
    string _errorMsg;
}
