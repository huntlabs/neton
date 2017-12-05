module store.util;
import std.stdio;
import std.array;
import std.string;
import std.algorithm.searching;
import std.json;

const string service_prefix = "/service/";
// if the key is "/foo/bar", it will produces result with path "/",
// "/foo" and "/foo/bar"

string getSafeKey(string key)
{
    string result;
    key = strip(key);
    if(endsWith(key,"/"))
		key = key[0..$-1];
	if(key.length == 0)
	{	
        result = "/";
	}
    else
        result = key;
    return result;
}

string[] getAllParent(string key)
{
    string[] result;
	key = strip(key);
	if(endsWith(key,"/"))
		key = key[0..$-1];
	if(key.length == 0)
	{	
		result ~= "/";
		return result;
	}
    auto segments = split(key, "/"); 
    for(int i = 0; i < segments.length; i++) {
        string path ;
        for(int j =0; j <= i ;j++)
        {   
            path ~= "/" ;
            path ~= segments[j];
        }
        if(path.length > 1)
            result ~= path[1..$];
        else
            result ~= path;
    }
	return result;
}


string getParent(string key)
{   
    string res;
    auto allP = getAllParent(key);
    if(allP.length > 1)
        res = allP[$-1-1];
    else
    {
        res = allP[0];
    }
    if(res == key)
        return string.init;
    else
        return res;
}

JSONValue tryGetJsonFormat(string json)
{
    JSONValue value;
    try
    {
        value = parseJSON(json);
    }
    catch(std.json.JSONException e)
    {
        return JSONValue(json);
    }
    return value;
}