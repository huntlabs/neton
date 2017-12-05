module store.RocksdbStore;

import protocol.Msg;
import zhang2018.common.Serialize;
import core.stdc.stdio;
import std.string;
import std.stdio;
import std.json;
import std.experimental.allocator;
import std.file;
import raft.Node;
import zhang2018.common.Log;

import rocksdb.database;
import rocksdb.options;
import std.conv : to;
import store.util;
import std.algorithm.searching;
import std.algorithm.mutation;

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
		"value" : "some .."
 }
  key : {
	  "dir" : "true",
	  "children" : "/child1;/child2"
  }
 */
class RocksdbStore
{
	this(ulong ID)
	{
		auto opts = new DBOptions;
        opts.createIfMissing = true;
        opts.errorIfExists = false;

        _rocksdb = new Database(opts, "rocks.db"~ to!string(ID));
		init();
	}

	void init()
	{
		auto j = getJsonValue("/");
		if(j.type == JSON_TYPE.NULL)
		{
			JSONValue dirvalue;
			dirvalue["dir"] = "true";
			dirvalue["children"] = "";

			SetValue("/",dirvalue.toString);
		}
	}

	bool Exsit(string key)
	{
		return getJsonValue(key).type != JSON_TYPE.NULL;
	}

	//递归创建目录
	void createDirWithRecur(string dir)
	{
		log_info("---create dir : ",dir);
		auto parent = getParent(dir);
		if(parent != string.init)
		{
			if(!Exsit(parent))
			{
				createDirWithRecur(parent);
			}
			CreateAndAppendSubdir(parent,dir);
		}
		else
		{
			CreateAndAppendSubdir(dir,"");
		}
	}

	//创建子目录 并添加key到父目录，父目录不存在则创建
	void CreateAndAppendSubdir(string parent , string child)
	{
		if(child.length > 0)
		{
			JSONValue subdir;
			subdir["dir"] = "true";
			subdir["children"] = "";
			SetValue(child,subdir.toString);
		}
		

		auto j = getJsonValue(parent);
		if(j.type != JSON_TYPE.NULL)
		{
			auto childs = j["children"].str;
			auto segments = split(childs, ";"); 
			if(find(segments,child).empty)
			{
				childs ~= ";";
				childs ~= child;
				j["children"] = childs;

				SetValue(parent,j.toString);
			}
		}
		else
		{
			JSONValue dirvalue;
			dirvalue["dir"] = "true";
			dirvalue["children"] = child;

			SetValue(parent,dirvalue.toString);
		}
	}

	string Lookup(string key)
	{
		if(key.length == 0)
			return string.init;
		if(_rocksdb is null)
			return string.init;
		
		return _rocksdb.getString(key);
	}

	void Remove(string key,bool recursive = false)
	{
		if(!recursive)
			_rocksdb.removeString(key);
		else
		{
			auto j = getJsonValue(key);
			if(j.type == JSON_TYPE.OBJECT && j["dir"].str == "true")
			{
				auto children = j["children"].str();
				auto segments = split(children, ";"); 
				foreach(subkey;segments)
				{
					if(subkey.length >0)
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
		//log_info("---getJsonValue key : ",key,"  value : ",jsonvalue);
		JSONValue jvalue;
		if(jsonvalue.length == 0)
			return jvalue;
		try
        {
			//log_info("parse json value : ",jsonvalue);
            jvalue = parseJSON(jsonvalue);
        }
        catch (Exception e)
        {
            log_error("catch  error : %s", e.msg);
        }

		return jvalue;
	}

	string getStringValue(string key)
	{
		return Lookup(key);
	}


	bool set(string nodePath, string value ,out string error)
	{
		//不能是目录
		auto node = getJsonValue(nodePath);
		if(node.type == JSON_TYPE.OBJECT && "dir" in node)
		{
			if(node["dir"].str == "true")
			{
				error ~= nodePath;
				error ~= "  is dir";
				return false;
			}
		}
		//父级目录要么不存在  要么必须是目录
		auto p = getParent(nodePath);
		if(p == string.init)
		{
			error = "the key is illegal";
			return false;
		}
		auto j = getJsonValue(p);
		if(j.type != JSON_TYPE.NULL && j["dir"].str != "true")
		{
			error ~= p;
			error ~= " not is dir";
			return false;
		}


		setFileKeyValue(nodePath,value);
		return true;
	}

	bool createDir(string path,out string error)
	{
		//目录或文件存在
		auto node = getJsonValue(path);
		if(node.type != JSON_TYPE.NULL )
		{
			error ~= path;
			error ~= " is exist";
			return false;
		}

		//父级目录要么不存在  要么必须是目录
		auto p = getParent(path);
		if(p == string.init)
		{
			error = "the key is illegal";
			return false;
		}
		auto j = getJsonValue(p);
		if(j.type != JSON_TYPE.NULL && j["dir"].str != "true")
		{
			error ~= p;
			error ~= " not is dir";
			return false;
		}

		createDirWithRecur(path);
		return true;
	}

	protected :
		void SetValue(string key , string value)
		{
			_rocksdb.putString(key,value);
		}

		void setFileKeyValue(string key , string value)
		{
			//log_info("---key : ",key);
			auto p = getParent(key);
			//log_info("---parent key : ",p);
			if(p != string.init)
			{
				if(!Exsit(p))
					createDirWithRecur(p);
			}
			auto j = getJsonValue(p);
			log_info("----json type :",j.type);
			auto children = j["children"].str();
			auto segments = split(children, ";"); 
			if(find(segments,key).empty)
			{
				children ~= ";";
				children ~= key;
				j["children"] = children;
		        SetValue(p,j.toString);
			}

			JSONValue filevalue;
			filevalue["dir"] = "false";
			filevalue["value"] = value;

			SetValue(key,filevalue.toString);		
		}

		void removekeyFromParent(string skey)
		{
			auto pkey = getParent(skey);
			if(pkey != string.init)
			{
				auto pnode = getJsonValue(pkey);
				if(pnode.type == JSON_TYPE.OBJECT && "children" in pnode)
				{
					auto children = pnode["children"].str;
					auto segments = split(children, ";"); 
					auto childs = remove!(a => a == skey)(segments);
					string newvalue;
					foreach(child; childs) {
						if(child != ";" && child.length >0)
						{
							newvalue ~= child;
							newvalue ~= ";";
						}
					}
					pnode["children"] = newvalue;
					SetValue(pkey,pnode.toString);
				}
			}
		}

    private :
			Database _rocksdb;

}
