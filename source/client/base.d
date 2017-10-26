module client.base;

import zhang2018.dreactor.aio.AsyncTcpBase;
import zhang2018.dreactor.event.Poll;
import zhang2018.common.Log;
import zhang2018.common.Serialize;
import core.stdc.string;
import protocol.Msg;
import server.NetonServer;

class base : AsyncTcpBase
{

	enum PACK_HEAD_LEN = 4;

	this(Poll poll ,ulong ID, byte[] buffer)
	{
		super(poll);
		readBuff = buffer;
		_ID = ID;
	}

	override bool doEstablished()
	{
		log_info( _ID ," have a connect");
		return true;
	}

	//pack - 4 len
	//body - 
	override bool doRead(byte[] data , int length)
	{
	
		int pos = 0;
		while(pos < length)
		{
			if(_packLen == 0)
			{
				auto left = length - pos;
				if(left + _cur_header_len < PACK_HEAD_LEN)	
				{
		
					_header[_cur_header_len .. left + _cur_header_len] = data[pos .. pos + left];
	
					_cur_header_len += left;
	
					return true;
	
				}
				else
				{
			
					_header[_cur_header_len .. PACK_HEAD_LEN] = data[pos .. pos +  PACK_HEAD_LEN - _cur_header_len];
	
					pos += PACK_HEAD_LEN - _cur_header_len;
	
					memcpy(&_packLen , _header.ptr , PACK_HEAD_LEN);
	
				}
			}
			else
			{
				auto left = length - pos;
				ulong cur_packlen = _packBuff.length;
				if(left + cur_packlen < _packLen)
				{
					_packBuff ~= data[pos .. length];
					return true;
				}
				else
				{
					_packBuff ~= data[pos .. _packLen - cur_packlen + pos];
					pos += _packLen - cur_packlen;

					Message msg = deserialize!Message(_packBuff);
					// if(msg.Type != MessageType.MsgHeartbeat && msg.Type != MessageType.MsgAppResp)
					// 	log_debug(_ID , " recv a msg " , msg);
					NetonServer.instance.Step(msg);
					_packBuff.length = 0;
					_packLen = 0;
					_cur_header_len = 0;

				}
			}
		}


		return true;
	}

	private	int				_packLen;
	private	byte[]			_packBuff;	
	ulong 					_ID;
	byte[PACK_HEAD_LEN]		_header;
	int						_cur_header_len;
}

