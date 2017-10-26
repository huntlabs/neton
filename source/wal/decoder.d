module wal.decoder;

import std.stdio;
import wal.record;
import zhang2018.common.Log;
import zhang2018.common.Serialize;	
// const minSectorSize = 512;

// frameSizeBytes is frame size in bytes, including record size and padding size.
const int frameSizeBytes = 8;

class Decoder  {

	// lastValidOff file offset following the last valid decoded record
	long _lastValidOff;
    byte[] _uint64buf;
	// crc          hash.Hash32
    File[]  _fr;

    this(File[] fp)
    {
        _fr = fp;
        _lastValidOff = 0;
        // if(_fr.length > 0)
        //     _fr[0].seek(0,SEEK_SET);
    }

    int decode(out Record rec)
    {   
         //log_info("decode read fd length is ",_fr.length);
        int res = 0;
        if(_fr.length == 0)
        {
            log_error("decode read fd length is 0 ..");
            return -1;
        }

        ulong dataLen = readUint64();
        if(dataLen == 0)
        {   
            _fr[0].close;
            _fr = _fr[1..$];
            if (_fr.length == 0) {
                log_warning("next decode read fd length is 0 ..");
                return -1;
            }
            //_fr[0].seek(0,SEEK_SET);
            _lastValidOff = 0;
            return decode(rec);
        }

        auto type = cast(WalType)(_uint64buf[0]);
        if(type != WalType.recordType)
            return -1;
        auto buf = new byte[dataLen];
        auto data = _fr[0].rawRead(buf);
        if(data.length != dataLen)
        {
            log_error("decode error : read data ");
            return -1;
        }

        rec = deserialize!Record(data);
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
              //log_info("read fd open : ",_fr[0].isOpen," filename : ",_fr[0].name);
              result = _fr[0].rawRead(_uint64buf);
         }
         catch(Exception e)
         {
            log_error("read file exception  ", e.msg, " fd open : ",_fr[0].isOpen," filename : ",_fr[0].name);
             return 0;
         }
        if(result.length != _uint64buf.length)
        {
            log_warning("read file is eof ..");
            return 0;
        }
        ulong len =0;
        for(int i =1 ; i < _uint64buf.length; i++)
        {
            len |= (((_uint64buf[i] << (8-i-1)) & 0xffffffffffffffff));
        }
        return len;
     }

     long lastOffset()  { return _lastValidOff; }

     void Close()
     {
         foreach(fp;_fr)
		{
			fp.close();
		}
     }
}
