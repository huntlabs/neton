module wal.encoder;
import std.file;
import wal.record;
import zhang2018.common.Serialize;
import std.stdio;
import zhang2018.common.Log;
// walPageBytes is the alignment for flushing records to the backing Writer.
// It should be a multiple of the minimum sector size so that WAL can safely
// distinguish between torn writes and ordinary data corruption.
//const walPageBytes = 8 * minSectorSize;

/* encode form 

 | 8 bit |   56 bit    | data | ...... | 8 bit |   56 bit    | data | .......
     |         |
 | type  | data size   |               | type  | data size   |
 -----------------------               -----------------------
          ||                                      ||
       uint64(8 byte)                       uint64(8 byte)

*/

class Encoder  {

	byte[]    _buf;
    File      _fw;


    this(File fp) {
        _fw = fp;
       // _buf = new byte[1024*1024];
    }


    void  encode(Record rec)  {
        
        //_fw.seek(0,SEEK_END);
        byte[] data;

        data = serialize(rec);
        //log_info("--------------debug 3-----",data," file name : ",_fw.name);
        ulong lenField= encodeFrameSize(data.length,WalType.recordType);
        // log_info("--------------debug 4-----",lenField," file name : ",_fw.name);
        writeUint64(lenField);
        //log_info("--------------debug 5-----"," file name : ",_fw.name);
        _fw.rawWrite(data);
        return;
    }

    ulong encodeFrameSize(ulong dataBytes, WalType type) {
        ulong t = cast(ulong)type;
        return (cast(ulong)(t << 56) | dataBytes);
    }

    void flush()  {
        //log_info("----- flush file : ",_fw.name," file open : ",_fw.isOpen);
        if(_fw.isOpen)
            _fw.flush();
    }

    void  writeUint64(ulong n)  {
        byte[] uint64buf = new byte[8];
        for(int i = 0 ; i< 8 ; i++ )
            uint64buf[i] =cast(byte)( (cast(byte)(n >>(8*(8-i-1)))) & 0xff);
        //log_info("--------------debug 6-----",uint64buf," file name : ",_fw.name, " file open : ",_fw.isOpen);
        try
        {
            if(!_fw.isOpen)
                _fw.open(_fw.name,"ab+");
            _fw.rawWrite(uint64buf);
            // log_info("--------------debug 7-----");
        }
        catch (Exception e)
        {
            log_error("-----7 catch :", e.msg);
        }

        return;
    }

    ulong curOff()
    {
        return _fw.tell();
    }
}

