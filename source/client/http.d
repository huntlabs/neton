module client.http;
import zhang2018.dreactor.aio.AsyncTcpBase;
import zhang2018.dreactor.event.Poll;
import zhang2018.common.Log;
import server.NetonServer;
import std.string;
import std.conv;
import std.net.curl;
import protocol.Msg;
import server.NetonConfig;
import store.util;
import std.json;

enum MAX_HTTP_REQUEST_BUFF = 1024 * 16;



enum RequestMethod
{
	METHOD_GET = 0, 
	METHOD_SET = 1,
	METHOD_POST = 2,
	METHOD_PUT = 3,
	METHOD_DELETE = 4,
	METHOD_UNKNOWN = 5,
};


struct RequestCommand
{
	RequestMethod Method;
	string		  Key;
	string		  Value;
	size_t		  Hash;
	string 		  Params;
};




class http : AsyncTcpBase
{
	this(Poll poll ,   byte[] buffer)
	{
		super(poll);
		readBuff = buffer;
	}

	bool is_request_finish(ref bool finish, ref string url , ref string strbody)
	{
		import std.typecons : No;
		
		string str = cast(string)_buffer;
		long header_pos = indexOf(str , "\r\n\r\n");
		
		if( header_pos == -1)
		{
			finish = false;
			return true;
		}
		
		string strlength = "content-length: ";
		int intlength = 0;
		long pos = indexOf(str , strlength , 0 , No.caseSensitive);
		if( pos != -1)
		{
			long left = indexOf(str , "\r\n" , cast(size_t)pos);
			if(pos == -1)
				return false;
			
			strlength = cast(string)_buffer[cast(size_t)(pos + strlength.length) .. cast(size_t)left];
			intlength = to!int(strlength);
		}
		
		
		if(header_pos + 4 + intlength == _buffer.length)
		{
			finish = true;
		}
		else
		{
			finish = false;
			return true;
		}
		
		long pos_url = indexOf(str , "\r\n");
		if(pos_url == -1)
			return false;
		
		auto strs = split(cast(string)_buffer[0 .. cast(size_t)pos_url]);
		if(strs.length < 3)
			return false;
		auto methond = toUpper(strs[0]);
		switch(methond)
		{
			case "GET":
				_requestMethod = RequestMethod.METHOD_GET;
				break;
			case "PUT":
				_requestMethod = RequestMethod.METHOD_PUT;
				break;
			case "POST":
				_requestMethod = RequestMethod.METHOD_POST;
				break;
			case "DELETE":
				_requestMethod = RequestMethod.METHOD_DELETE;
				break;
			default:
				_requestMethod = RequestMethod.METHOD_UNKNOWN;
				break;
		}
		url = strs[1];
		strbody = cast(string)_buffer[cast(size_t)(header_pos + 4) .. $];
		
		return true;
	}

	bool do_response(string strbody)
	{
		auto res = log_format("HTTP/1.1 200 OK\r\nServer: kiss\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s"
			, strbody.length , strbody);
		auto ret = doWrite(cast(byte[])res , null , 
			delegate void(Object obj){
				close();
			});
		
		//log_info(" do_response " , ret);

		if(ret == 1)
			return false;

		return true;
	}

	bool process_request(string url , string strbody)
	{
		if(!NetonServer.instance()._node.isLeader() && _requestMethod != RequestMethod.METHOD_GET)
		{
			// auto leader = NetonServer.instance.leader();
			// log_info("leader id : ", leader);
			// auto http = HTTP();

			// // Put with data senders
			// auto msg = strbody;
			// http.contentLength = msg.length;
			// http.onSend = (void[] data)
			// {
			// 	auto m = cast(void[])msg;
			// 	size_t len = m.length > data.length ? data.length : m.length;
			// 	if (len == 0) return len;
			// 	data[0..len] = m[0..len];
			// 	msg = msg[len..$];
			// 	return len;
			// };

			// // Track progress
			// if(_requestMethod == RequestMethod.METHOD_GET)
			// 	http.method = HTTP.Method.get;
			// else if(_requestMethod == RequestMethod.METHOD_PUT)
			// 	http.method = HTTP.Method.put;
			// else if(_requestMethod == RequestMethod.METHOD_DELETE)
			// 	http.method = HTTP.Method.del;
			// string urlpath = "http://";
			// foreach(peer;NetonConfig.instance.peersConf)
			// {
			// 	if(peer.id == leader)
			// 	{
			// 		urlpath ~= peer.ip;
			// 		urlpath ~= ":";
			// 		urlpath ~= to!string(peer.apiport);
			// 	}
			// }
			// http.url = urlpath ~ url;
			// http.onReceive = &this.onHttpRecive;
			// http.onProgress = &this.onHttpProgress;
			// http.perform();

			// NetonServer.instance.saveHttp(this);
			JSONValue  res;
			try
			{
				res["action"] = "not leader";
				
				JSONValue  leader;
				auto id = NetonServer.instance.leader();
			    leader["id"] = id;
				foreach(peer;NetonConfig.instance.peersConf)
				{
					if(peer.id == id)
					{
						leader["ip"] = peer.ip;
						leader["apiport"]= peer.apiport;
						break;
					}
				}

				res["leader"] = leader;
			}
			catch (Exception e)
			{
				log_error("catch %s", e.msg);
				res["error"] = e.msg;
			}
			return do_response(res.toString);	
		}

		_params.clear;
		if(strbody.length > 0){
			
			auto keyvalues = split(strbody, "&");
			foreach( k ; keyvalues)
			{
				auto kv = split(k , "=");
				if(kv.length == 2)
					_params[kv[0]] = kv[1];
			}
		}

		string path;
		long pos = indexOf(url , "?");
		if(pos == -1){
			path = url;
		}
		else{
			path = url[0 .. pos];
			auto keyvalues = split(url[pos + 1 .. $] , "&");
			foreach( k ; keyvalues)
			{
				auto kv = split(k , "=");
				if(kv.length == 2)
					_params[kv[0]] = kv[1];
			}
			url = path;
		}
		

		_hash = this.toHash();

		if(!startsWith(url,"/keys"))
		{
			return do_response("can not sovle " ~ url);
		}

		if(url.length >= 5)
		  url = url[5..$];
		url = getSafeKey(url);

		JSONValue jparam;
		foreach(key,value;_params)
			jparam[key] = value;
		if(_requestMethod == RequestMethod.METHOD_GET)
		{
			// auto key = "key" in _params;
			 if(url.length == 0)
				return do_response("params key must not empty");
	
			RequestCommand command = { Method:RequestMethod.METHOD_GET , Key: url , Hash:_hash,Params:jparam.toString};
			NetonServer.instance().Propose(command , this);
			return true;
		}
		else if(_requestMethod == RequestMethod.METHOD_PUT)
		{
			if( url.length == 0)
				return do_response("params key  must not empty ");

			RequestCommand command = { Method:RequestMethod.METHOD_PUT , Key: url ,Hash:_hash,Params:jparam.toString};
			NetonServer.instance().Propose(command , this);
			return true;
		}
		else if(_requestMethod == RequestMethod.METHOD_DELETE)
		{
			// auto key = "key" in _params;
			 if(url.length == 0)
				return do_response("params key must not empty");

			RequestCommand command = { Method:RequestMethod.METHOD_DELETE , Key: url , Hash:_hash,Params:jparam.toString};
			NetonServer.instance().Propose(command , this);
			return true;
		}
		// else if(path == "/add")
		// {
		// 	auto nodeID = "ID" in _params;
		// 	auto Context = "Context" in _params;
		// 	if(nodeID == null || nodeID.length == 0 || Context.length == 0 || Context == null)
		// 		return do_response("ID or Context must not empty");

		// 	ConfChange cc = { NodeID : to!ulong(*nodeID) , Type : ConfChangeType.ConfChangeAddNode ,Context:*Context };
		// 	NetonServer.instance().ProposeConfChange(cc);
		// 	return do_response("have request this add conf");
			
		// }
		// else if(path == "/del")
		// {
		// 	auto nodeID = "ID" in _params;
		// 	if(nodeID == null || nodeID.length == 0)
		// 		return do_response("ID must not empty");
		// 	ConfChange cc = { NodeID : to!ulong(*nodeID) , Type : ConfChangeType.ConfChangeRemoveNode };
		// 	NetonServer.instance().ProposeConfChange(cc);
		// 	return do_response("have request this remove conf");
		// }
		else
		{
			return do_response("can not sovle " ~ url);
		}
	}


	override bool doRead(byte[] data , int length)
	{
		_buffer ~= data[0 .. length];
		bool finish;
		string strurl;
		string strbody;
		if(!is_request_finish(finish ,strurl,strbody ))
			return false;

		if(finish)
		{
			log_info("http request method :",_requestMethod);
			log_info("http request data：",strbody);
			return process_request(strurl , strbody);
		}
		else if(_buffer.length >= MAX_HTTP_REQUEST_BUFF)
		{
			return false;
		}

		return true;
	}

	override bool onClose()
	{
		//log_info("-----http close ----");
		NetonServer.instance.handleHttpClose(_hash);
		super.onClose();
		return true;
	}

	string[string] params()
	{
		return _params;
	}

	size_t onHttpRecive(ubyte[] data)
	{
		_httpRecvBuf ~= data;
		return data.length;
	}

	int onHttpProgress(size_t dlTotal, size_t dlNow, size_t ulTotal, size_t ulNow)
	{
		if(dlTotal == dlNow)
		{
			log_info("leader response data : ",cast(string)_httpRecvBuf);
			do_response(cast(string)_httpRecvBuf);
		}
		return 0;
	}

	private byte[] _buffer;
	private string[string] _params;
	private size_t 	_hash;
	private RequestMethod _requestMethod;
	private ubyte[] _httpRecvBuf;
}

