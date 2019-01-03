module neton.wal.decoder;

import std.stdio;
import neton.wal.record;
import hunt.logging;
import hunt.util.serialize;

// const minSectorSize = 512;

// frameSizeBytes is frame size in bytes, including record size and padding size.
const int frameSizeBytes = 8;

class Decoder
{

    // lastValidOff file offset following the last valid decoded record
    long _lastValidOff;
    byte[] _uint64buf;
    // crc          hash.Hash32
    File[] _fr;

    this(File[] fp)
    {
        _fr = fp;
        _lastValidOff = 0;
        // if(_fr.length > 0)
        //     _fr[0].seek(0,SEEK_SET);
    }

    int decode(out Record rec)
    {
        //logInfo("decode read fd length is ",_fr.length);
        int res = 0;
        if (_fr.length == 0)
        {
            logError("decode read fd length is 0 ..");
            return -1;
        }

        ulong dataLen = readUint64();
        if (dataLen == 0)
        {
            _fr[0].close;
            _fr = _fr[1 .. $];
            if (_fr.length == 0)
            {
                logWarning("next decode read fd length is 0 ..");
                return -1;
            }
            //_fr[0].seek(0,SEEK_SET);
            _lastValidOff = 0;
            return decode(rec);
        }

        auto type = cast(WalType)(_uint64buf[0]);
        if (type != WalType.recordType)
            return -1;
        auto buf = new byte[dataLen];
        auto data = _fr[0].rawRead(buf);
        if (data.length != dataLen)
        {
            logError("decode error : read data ");
            return -1;
        }
        //logInfo("----unserialize Record : ",data.length," ",data);
        rec = unserialize!Record(data);
        _lastValidOff += frameSizeBytes + dataLen;
        return res;
    }

    ulong readUint64()
    {
        _uint64buf.length = 0;
        _uint64buf = new byte[8];
        byte[] result;
        try
        {
            //logInfo("read fd open : ",_fr[0].isOpen," filename : ",_fr[0].name);
            result = _fr[0].rawRead(_uint64buf);
        }
        catch (Exception e)
        {
            logError("read file exception  ", e.msg, " fd open : ",
                    _fr[0].isOpen, " filename : ", _fr[0].name);
            return 0;
        }
        if (result.length != _uint64buf.length)
        {
            logWarning("read file is eof ..");
            return 0;
        }

        ulong len = 0;
        byte[] src = _uint64buf.dup;
        src[0] = 0;
        int offset = 0;
        len = cast(ulong)((cast(ulong)(src[offset] & 0xFF) << 56) | (
                cast(ulong)(src[offset + 1] & 0xFF) << 48) | (cast(ulong)(
                src[offset + 2] & 0xFF) << 40) | (cast(ulong)(
                src[offset + 3] & 0xFF) << 32) | (cast(ulong)(
                src[offset + 4] & 0xFF) << 24) | (cast(ulong)(
                src[offset + 5] & 0xFF) << 16) | (cast(ulong)(
                src[offset + 6] & 0xFF) << 8) | cast(ulong)(src[offset + 7] & 0xFF));

        //logInfo("------read head  :  ",_uint64buf, " len : ",len);
        return len;
    }

    long lastOffset()
    {
        return _lastValidOff;
    }

    void Close()
    {
        foreach (fp; _fr)
        {
            fp.close();
        }
    }
}
