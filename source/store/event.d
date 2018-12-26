module store.event;

import store.NodeExtern;
import std.experimental.allocator;
import std.json;

enum ServiceState{
    Passing   = "passing",
    Warning   = "warning",
    Critical  = "critical",
    Maintenance = "maintenance",
}

enum EventAction {
	Get              = "get",
	Create           = "create",
	Set              = "set",
	Update           = "update",
	Delete           = "delete",
    Register         = "register",
    Deregister       = "deregister",
	CompareAndSwap   = "compareAndSwap",
	CompareAndDelete = "compareAndDelete",
	Expire           = "expire",
}

class Event{

    this()
    {

    }
    this(EventAction action, string key, ulong modifiedIndex,bool recursive = false) {

        _node = new NodeExtern(key,modifiedIndex,recursive);
        _action = action;
        _refresh = false;
    }

    void opAssign(S)(auto ref S e) if(is(S == Unqual!(typeof(this))))
    {
        _action = e._action;
        _netonIndex = e._netonIndex;
        _refresh = e._refresh;
        _node = e._node.Clone();
        _prevNode = e._prevNode.Clone();
        _errorMsg = e._errorMsg;
    }

    bool IsCreated()  {
        if (this._action == EventAction.Create) {
            return true;
        }
        return _action == EventAction.Set && _prevNode is null;
    }

    @property ulong Index()  {
        return /*_node.modifiedIndex*/_netonIndex;
    }


    @property EventAction action()  {
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

    JSONValue nodeValue()
    {
        return _node.value;
    }

     @property ulong netonIndex()  {
        return _netonIndex;
    }

    void setNetonIndex(ulong idx)
    {
        _netonIndex = idx;
    }

    Event Clone() {
        auto e = theAllocator.make!Event();
        e = this;
        return  e;
    }

    @property bool dir()
    {
        return _node.dir;
    }

    @property bool refresh()
    {
        return _refresh;
    }

    void  SetRefresh() {
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

    void  setErrorMsg(string msg) {
        _errorMsg = msg;
    }

    string rpcValue()
    {
        if (this.error.length > 0)
			return this.error;
		return this.nodeValue().toString;
    }

    private :
        	EventAction _action;
            NodeExtern  _node;
            NodeExtern  _prevNode;
            ulong       _netonIndex;      
            bool        _refresh;
            string      _errorMsg;
}
