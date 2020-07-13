module neton.store.RocksdbStore;

import core.stdc.stdio;
import std.string;
import std.stdio;
import std.json;
import std.experimental.allocator;
import std.file;
import std.conv : to;
import std.algorithm.searching;
import std.algorithm.mutation;

import hunt.logging;
import hunt.util.DateTime;
import hunt.util.Serialize;

import rocksdb.database;
import rocksdb.options;

import neton.store.Util;
import neton.lease;
import neton.protocol.neton;
import neton.protocol.neton;
import neton.rpcservice.Command;

/*
 * service register/deregister store format
 *
 * key(/redis/redis1) : {
	 "dir" : "false",
	 "value" : "{
		 "service" : {
		 "id" : "redis1",
		 "name" : "redis",
		 "address" : "127.0.0.1",
		 "port" : 8000
	 	},
		"check" : {
			"http" : "http://127.0.0.1:8001",
			"interval" : 10,
			"timeout" : 30
		},
		"status" : "passing"
	 }"	 
 }
 */

/* 
 * k/v store format
 *
 * key : {
	 	"dir" : "false",
		"value" : "some ..",
		"leaseID": long.init,
		"create_time" : 2333334422 ///unix seconds timestamp
 }
  key : {
	  "dir" : "true",
	  "children" : "/child1;/child2"
  }
 */

/***
   * ****************  Lease  **************
   * key : /lease/{leaseID}
   * value : {
	   			"dir" : "false",
   *			"id"  : {leaseID},
   *			"ttl" : 12344, ///time to live
   *			"create_time" : 13444244, ///unix seconds timestamp
   *	        "items":[ 
   *	   					{
   *	   						"key" : "attach key",
   *							"time" : "attach time"
   *						}
   *					]
   *         }
   ***/

/***    Gen Lease ID
	key : LEASE_GEN_ID_PREFIX
	value : {
		  		"ID" : 1234
			}
 ****/

class RocksdbStore
{

	private
	{
		Database _rocksdb;
		Lessor _lessor;
	}

	this(ulong ID, Lessor l)
	{
		auto opts = new DBOptions;
		opts.createIfMissing = true;
		opts.errorIfExists = false;

		_rocksdb = new Database(opts, "rocks.db" ~ to!string(ID));
		_lessor = l;
		init(l);
	}

	void init(Lessor lessor = null)
	{
		auto j = getJsonValue("/");
		if (j.type == JSONType.null_)
		{
			JSONValue dirvalue;
			dirvalue["dir"] = "true";
			dirvalue["children"] = "";

			SetValue("/", dirvalue.toString);
		}

		///init Lease
		if (lessor !is null)
		{
			auto leases = getJsonValue(LEASE_PREFIX[0 .. $ - 1]);
			if (leases.type != JSONType.null_)
			{
				auto lease_keys = leases["children"].str;
				if (lease_keys.length > 0)
				{
					auto leaseIDs = split(lease_keys, ";");
					foreach (leaseID; leaseIDs)
					{
						if (leaseID.length != 0)
						{
							auto leaseItem = getJsonValue(leaseID);
							if (leaseItem.type != JSONType.null_)
							{
								Lease l = new Lease();
								l.ID = leaseItem["id"].integer;
								l.ttl = leaseItem["ttl"].integer;
								l.expiry = l.ttl + leaseItem["create_time"].integer;
								foreach (item; leaseItem["items"].array)
								{
									l.itemSet.add(item["key"].str);
								}
								lessor.init(l);
							}
						}
					}
				}
			}
		}

	}

	bool isDir(string key)
	{
		key = getSafeKey(key);
		auto value = getJsonValue(key);
		if (value.type != JSONType.null_)
		{
			if ("dir" in value)
			{
				if (value["dir"].str == "true")
					return true;
			}
		}
		return false;
	}

	bool Exsit(string key)
	{
		return getJsonValue(key).type != JSONType.null_;
	}

	//递归创建目录
	void createDirWithRecur(string dir)
	{
		logInfo("---create dir : ", dir);
		auto parent = getParent(dir);
		if (parent != string.init)
		{
			if (!Exsit(parent))
			{
				createDirWithRecur(parent);
			}
			CreateAndAppendSubdir(parent, dir);
		}
		else
		{
			CreateAndAppendSubdir(dir, "");
		}
	}

	//创建子目录 并添加key到父目录，父目录不存在则创建
	void CreateAndAppendSubdir(string parent, string child)
	{
		if (child.length > 0)
		{
			JSONValue subdir;
			subdir["dir"] = "true";
			subdir["children"] = "";
			SetValue(child, subdir.toString);
		}

		auto j = getJsonValue(parent);
		if (j.type != JSONType.null_)
		{
			auto childs = j["children"].str;
			auto segments = split(childs, ";");
			if (find(segments, child).empty)
			{
				childs ~= ";";
				childs ~= child;
				j["children"] = childs;

				SetValue(parent, j.toString);
			}
		}
		else
		{
			JSONValue dirvalue;
			dirvalue["dir"] = "true";
			dirvalue["children"] = child;

			SetValue(parent, dirvalue.toString);
		}
	}

	string Lookup(string key)
	{
		if (key.length == 0)
			return string.init;
		if (_rocksdb is null)
			return string.init;

		return _rocksdb.getString(key);
	}

	void Remove(string key, bool recursive = false)
	{
		key = getSafeKey(key);
		if (!recursive)
			_rocksdb.removeString(key);
		else
		{
			auto j = getJsonValue(key);
			if (j.type == JSONType.object && j["dir"].str == "true")
			{
				auto children = j["children"].str();
				auto segments = split(children, ";");
				foreach (subkey; segments)
				{
					if (subkey.length > 0)
						_rocksdb.removeString(subkey);
				}
			}

			_rocksdb.removeString(key);
		}
		removekeyFromParent(key);
	}

	JSONValue getJsonValue(string key)
	{
		auto jsonvalue = Lookup(key);
		// logInfo("---getJsonValue key : ",key,"  value : ",jsonvalue);
		JSONValue jvalue;
		if (jsonvalue.length == 0)
			return jvalue;
		try
		{
			//logInfo("parse json value : ",jsonvalue);
			jvalue = parseJSON(jsonvalue);
		}
		catch (Exception e)
		{
			logError("catch  error : %s", e.msg);
		}

		return jvalue;
	}

	string getStringValue(string key)
	{
		return Lookup(key);
	}

	bool set(string originKey, string value, out string error, long leaseid = long.init)
	{
		//不能是目录
		auto nodePath = getSafeKey(originKey);
		auto node = getJsonValue(nodePath);
		if (node.type == JSONType.object && "dir" in node)
		{
			if (node["dir"].str == "true")
			{
				error ~= nodePath;
				error ~= "  is dir";
				logError("set error : ", error, "  info: ", node);
				return false;
			}
		}
		//父级目录要么不存在  要么必须是目录
		auto p = getParent(nodePath);
		if (p == string.init)
		{
			error = "the key is illegal";
			logError("set error : ", error, "  info: ", p);
			return false;
		}
		auto j = getJsonValue(p);
		if (j.type != JSONType.null_ && j["dir"].str != "true")
		{
			error ~= p;
			error ~= " not is dir";
			logError("set error : ", error, "  info: ", p);
			return false;
		}

		setFileKeyValue(originKey, value, leaseid);
		return true;
	}

	bool createDir(string path, out string error)
	{
		path = getSafeKey(path);
		//目录或文件存在
		auto node = getJsonValue(path);
		if (node.type != JSONType.null_)
		{
			error ~= path;
			error ~= " is exist";
			return false;
		}

		//父级目录要么不存在  要么必须是目录
		auto p = getParent(path);
		if (p == string.init)
		{
			error = "the key is illegal";
			return false;
		}
		auto j = getJsonValue(p);
		if (j.type != JSONType.null_ && j["dir"].str != "true")
		{
			error ~= p;
			error ~= " not is dir";
			return false;
		}

		createDirWithRecur(path);
		return true;
	}

	Lease grantLease(long leaseid, long ttl)
	{
		if (_lessor !is null)
		{
			auto l = _lessor.Grant(leaseid, ttl);
			if (l is null)
				return null;

			auto lease = getJsonValue(LEASE_PREFIX ~ leaseid.to!string);
			if (lease.type == JSONType.null_)
			{
				JSONValue newLease;
				newLease["dir"] = "false";
				newLease["ttl"] = l.ttl;
				newLease["id"] = leaseid;
				JSONValue[] items;
				newLease["items"] = items;
				newLease["create_time"] = time();
				setLeaseKeyValue(LEASE_PREFIX ~ leaseid.to!string, newLease.toString);

				///update global leaseID
				JSONValue leaseID;
				leaseID["ID"] = leaseid;
				SetValue(LEASE_GEN_ID_PREFIX, leaseID.toString);
				return l;
			}
		}
		return null;
	}

	bool revokeLease(long leaseid)
	{
		if (_lessor.Revoke(leaseid) is null)
			Remove(LEASE_PREFIX ~ leaseid.to!string);
		else
			return false;
		return true;
	}

	bool attachToLease(string key, long leaseid)
	{
		auto lease = getJsonValue(LEASE_PREFIX ~ leaseid.to!string);
		if (lease.type != JSONType.null_)
		{
			if (_lessor !is null)
			{
				LeaseItem item = {Key:
				key};
				_lessor.Attach(leaseid, [item]);
			}

			if (!canFind!((JSONValue b, string a) => b["key"].str == a)(lease["items"].array, key))
			{
				JSONValue item;
				item["key"] = key;
				item["time"] = time();
				lease["items"].array ~= item;
				SetValue(LEASE_PREFIX ~ leaseid.to!string, lease.toString);
			}
			return true;
		}
		return false;
	}

	bool detachFromLease(string key, long leaseid)
	{
		auto lease = getJsonValue(LEASE_PREFIX ~ leaseid.to!string);
		if (lease.type != JSONType.null_)
		{
			if (_lessor !is null)
			{
				LeaseItem item = {Key:
				key};
				_lessor.Detach(leaseid, [item]);
			}

			JSONValue[] oldItems = lease["items"].array;
			JSONValue[] newItems;
			foreach (JSONValue item; oldItems)
			{
				if (item["key"].str != key)
				{
					newItems ~= item;
				}
			}
			lease["items"] = newItems;
			SetValue(LEASE_PREFIX ~ leaseid.to!string, lease.toString);
			return true;
		}
		return false;
	}

	bool foreverKey(string key)
	{
		auto value = getJsonValue(key);
		if (value.type != JSONType.null_)
		{
			value["leaseID"] = long.init;
			SetValue(key, value.toString);
			return true;
		}
		return false;
	}

	long generateLeaseID()
	{
		auto value = getJsonValue(LEASE_GEN_ID_PREFIX);
		if (value.type != JSONType.null_)
		{
			return value["ID"].integer + 1;
		}
		else
			return 1;
	}

	LeaseTimeToLiveResponse leaseTimeToLive(long leaseid)
	{
		auto leaseItem = getJsonValue(LEASE_PREFIX ~ leaseid.to!string);
		LeaseTimeToLiveResponse respon = new LeaseTimeToLiveResponse();
		if (leaseItem.type != JSONType.null_)
		{
			respon.ID = leaseid;
			auto remainTTL = (leaseItem["create_time"].integer + leaseItem["ttl"].integer - time());
			respon.TTL = remainTTL > 0 ? remainTTL : 0;
			respon.grantedTTL = leaseItem["ttl"].integer;
			foreach (item; leaseItem["items"].array)
			{
				respon.keys ~= cast(ubyte[])(item["key"].str);
			}
		}
		else
		{
			respon.ID = leaseid;
			respon.TTL = -1;
			respon.grantedTTL = 0;
		}
		return respon;
	}

	LeaseLeasesResponse leaseLeases()
	{
		auto leases = getJsonValue(LEASE_PREFIX[0 .. $ - 1]);
		if (leases.type != JSONType.null_)
		{
			auto lease_keys = leases["children"].str;
			LeaseLeasesResponse response = new LeaseLeasesResponse();
			if (lease_keys.length > 0)
			{
				auto leaseIDs = split(lease_keys, ";");
				foreach (leaseID; leaseIDs)
				{
					if (leaseID.length != 0)
					{
						auto leaseItem = getJsonValue(leaseID);
						if (leaseItem.type != JSONType.null_)
						{
							LeaseStatus ls = new LeaseStatus();
							ls.ID = leaseItem["id"].integer;
							response.leases ~= ls;
						}
					}
				}
			}
			return response;
		}
		return null;
	}

	LeaseKeepAliveResponse renewLease(RpcRequest req)
	{
		if (_lessor !is null)
		{
			auto newTTL = _lessor.Renew(req.LeaseID);
			if (newTTL > 0)
			{
				auto lease = getJsonValue(LEASE_PREFIX ~ req.LeaseID.to!string);
				if (lease.type != JSONType.null_)
				{
					lease["ttl"] = newTTL;
					lease["create_time"] = time();

					setLeaseKeyValue(LEASE_PREFIX ~ req.LeaseID.to!string, lease.toString);

					LeaseKeepAliveResponse respon = new LeaseKeepAliveResponse();
					respon.ID = req.LeaseID;
					respon.TTL = newTTL;
					return respon;
				}
			}
		}
		logWarning("-----renewLease fail --------");
		return null;
	}

	PutResponse put(RpcRequest req)
	{
		string nodePath;
		if(req.CMD == RpcReqCommand.ConfigPutRequest)
		{
			nodePath = getConfigKey(req.Key);
		}
		else if(req.CMD == RpcReqCommand.RegistryPutRequest)
		{
			nodePath = getRegistryKey(req.Key);
		}
		else
		{
			nodePath = getSafeKey(req.Key);
		}
		// Set new value
		string error;
		if (req.LeaseID != 0)
		{
			if (attachToLease(nodePath, req.LeaseID))
			{
				auto ok = set(nodePath, req.Value, error, req.LeaseID);
				if (ok)
				{
					PutResponse respon = new PutResponse();
					auto kv = new KeyValue();
					kv.key = cast(ubyte[])(req.Key);
					kv.value = cast(ubyte[])(req.Value);
					respon.prevKv = kv;
					return respon;
				}
			}
		}
		else
		{
			auto ok = set(nodePath, req.Value, error);
			if (ok)
			{
				PutResponse respon = new PutResponse();
				auto kv = new KeyValue();
				kv.key = cast(ubyte[])(req.Key);
				kv.value = cast(ubyte[])(req.Value);
				respon.prevKv = kv;
				return respon;
			}
		}
		return null;
	}

	DeleteRangeResponse deleteRange(RpcRequest req)
	{
		DeleteRangeResponse respon = new DeleteRangeResponse();

		string nodePath;
		if(req.CMD == RpcReqCommand.ConfigDeleteRangeRequest)
		{
			nodePath = getConfigKey(req.Key);
		}
		else if(req.CMD == RpcReqCommand.RegistryDeleteRangeRequest)
		{
			nodePath = getRegistryKey(req.Key);
		}
		else
		{
			nodePath = getSafeKey(req.Key);
		}

		if (Exsit(nodePath))
		{
			Remove(nodePath, true);
			respon.deleted = 1;
		}
		else
			respon.deleted = 0;
		return respon;
	}

protected:
	void SetValue(string key, string value)
	{
		_rocksdb.putString(key, value);
	}

	void setFileKeyValue(string originKey, string value, long leaseid = long.init)
	{
		auto key = getSafeKey(originKey);

		auto p = getParent(key);
		if (p != string.init)
		{
			if (!Exsit(p))
				createDirWithRecur(p);
		}
		auto j = getJsonValue(p);
		auto children = j["children"].str();
		auto segments = split(children, ";");
		if (find(segments, key).empty)
		{
			children ~= ";";
			children ~= key;
			j["children"] = children;
			SetValue(p, j.toString);
		}

		JSONValue filevalue;
		filevalue["dir"] = "false";
		filevalue["key"] = originKey;
		filevalue["value"] = value;
		filevalue["leaseID"] = leaseid;
		filevalue["create_time"] = time();

		SetValue(key, filevalue.toString);
	}

	void removekeyFromParent(string skey)
	{
		auto pkey = getParent(skey);
		if (pkey != string.init)
		{
			auto pnode = getJsonValue(pkey);
			if (pnode.type == JSONType.object && "children" in pnode)
			{
				auto children = pnode["children"].str;
				auto segments = split(children, ";");
				auto childs = remove!(a => a == skey)(segments);
				string newvalue;
				foreach (child; childs)
				{
					if (child != ";" && child.length > 0)
					{
						newvalue ~= child;
						newvalue ~= ";";
					}
				}
				pnode["children"] = newvalue;
				SetValue(pkey, pnode.toString);
			}
		}
	}

	void setLeaseKeyValue(string key, string jsonValue)
	{
		auto p = getParent(key);
		if (p != string.init)
		{
			if (!Exsit(p))
				createDirWithRecur(p);
		}
		auto j = getJsonValue(p);
		auto children = j["children"].str();
		auto segments = split(children, ";");
		if (find(segments, key).empty)
		{
			children ~= ";";
			children ~= key;
			j["children"] = children;
			SetValue(p, j.toString);
		}

		SetValue(key, jsonValue);
	}

}
