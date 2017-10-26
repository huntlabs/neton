module wal.util;

import std.string;
import std.format;
import std.file;
import std.stdio;
import std.array;
import core.stdc.stdio;
import std.path;
import std.algorithm.sorting;
import zhang2018.common.Log;

bool Exist(string dirpath)  {
	return exists(dirpath);
}

bool isEmptyDir(string dirpath)
{
    if(!Exist(dirpath))
        return true;
    string[] names;
    foreach (string name; dirEntries(dirpath, SpanMode.shallow))
    {
        names ~= name;
    }

    return names.length == 0;
}

string walName(ulong seq, ulong index)  {

    auto walName = appender!string();
    formattedWrite(walName, "%s-%s.wal",seq,index);
    return walName.data;
}

void renameDir(string srcpath ,string dstpath)
{
    core.stdc.stdio.rename(toStringz(srcpath),toStringz(dstpath));
}

bool cmp_walname_idx(string a , string b)
{
    ulong a_seq,a_idx,b_seq,b_idx;
    parseWalName(a,a_seq,a_idx);
    parseWalName(b,b_seq,b_idx);
    return a_idx < b_idx;
}

bool cmp_walname_seq(string a , string b)
{
    ulong a_seq,a_idx,b_seq,b_idx;
    parseWalName(a,a_seq,a_idx);
    parseWalName(b,b_seq,b_idx);
    return a_seq < b_seq;
}

//按index 由小到大排序,同样index 按seq由小到大排序
string[] readWalNames( string dirpath)  {
	string[] names;
    if(!Exist(dirpath))
        return names;
    foreach (string name; dirEntries(dirpath, SpanMode.shallow))
    {
        names ~= std.path.baseName(name);
    }
    multiSort!(cmp_walname_idx,cmp_walname_seq)(names);
    return names;
}

void parseWalName( string str ,out ulong seq ,out ulong index) {
	formattedRead(str,"%s-%s.wal",&seq,&index);
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
long searchIndex(string[] names, ulong index) {
    long res = -1;
    if(names.length == 0)
        return res;
    ulong seq,idx;
    log_info("wal names : ",names );
	for(auto i= names.length-1; i>=0; i--)
    {
        parseWalName(names[i],seq,idx);
        if(idx <= index)
        {
            res = i;
            break;
        }
    }
    return res;
}


string snapDirPath(ulong id)
{
    auto path = appender!string();
    formattedWrite(path, "%s-%s","./snap",id);
    return path.data;
}

string walDirPath(ulong id)
{
    auto path = appender!string();
    formattedWrite(path, "%s-%s","./wal",id);
    return path.data;
}