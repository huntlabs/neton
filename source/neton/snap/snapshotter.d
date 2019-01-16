module neton.snap.snapshotter;

import std.stdio;
import std.file;
import std.path;
import std.array;
import std.format;

// import protocol.Msg;
import hunt.util.Serialize;
import hunt.logging;
import std.algorithm.sorting;

// import raft.Node;
import hunt.raft;

const string snapSuffix = ".snap";

const string ErrNoSnapshot = "snap: no available snapshot";
const string ErrEmptySnapshot = "snap: empty snapshot";
const string ErrCRCMismatch = "snap: crc mismatch";

// struct  SnapData {
// 	string data;
// }

class Snapshotter
{
    string _dir;

    struct snapName
    {
        int term;
        int index;
    }

    this(string dir)
    {
        _dir = dir;
    }

    void SaveSnap(Snapshot snapshot)
    {
        if (IsEmptySnap(snapshot))
            return;
        auto writer = appender!string();
        formattedWrite(writer, "%s/%s-%s%s", _dir, snapshot.Metadata.Term,
                snapshot.Metadata.Index, snapSuffix);
        logInfo("savesnap file name : ", writer.data);
        auto b = serialize(snapshot);
        auto file = File(writer.data, "wb");
        file.rawWrite(b);
        file.close();

        auto snaps = snapNames();
        foreach (snap; snaps)
        {
            auto filepath = _dir ~ "/" ~ snap;
            if (filepath != writer.data)
            {
                remove(filepath);
            }
        }
    }

    Snapshot loadSnap()
    {
        Snapshot snapshot;
        string lastName = getLastSnapName();
        if (lastName is null)
        {
            logInfo("-------- no snap shot info ");
            return snapshot;
        }
        auto file = File(lastName, "rb");
        byte[] content;
        byte[] data = new byte[1024];
        while (!file.eof())
        {
            content ~= file.rawRead(data);
        }
        file.close();
        snapshot = unserialize!Snapshot(content);
        return snapshot;
    }

    string[] snapNames()
    {
        string[] names;

        foreach (string name; dirEntries(_dir, SpanMode.shallow))
        {
            names ~= std.path.baseName(name);
        }
        return names;
    }

    string getLastSnapName()
    {
        string[] names = snapNames();
        if (names.length == 0)
            return string.init;
        snapName[] snap;
        foreach (string name; names)
        {
            int term, index;
            formattedRead(name, "%s-%s.snap", &term, &index);

            snap ~= snapName(term, index);
        }

        multiSort!("a.term > b.term", "a.index > b.index")(snap);
        logInfo("last snap shot ,term : ", snap[0].term, " index : ", snap[0].index);
        auto lastName = appender!string();
        formattedWrite(lastName, "%s/%s-%s%s", _dir, snap[0].term, snap[0].index, snapSuffix);
        return lastName.data;
    }
}
