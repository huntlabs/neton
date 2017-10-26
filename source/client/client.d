module client.client;

import zhang2018.dreactor.aio.AsyncTcpClient;
import protocol.Msg;
import zhang2018.common.Serialize;
import core.stdc.string;
//import network.node;

import zhang2018.common.Log;

import zhang2018.dreactor.event.Poll;

class NodeClient : AsyncTcpClient
{
	this(Poll poll , ulong srcID , ulong dstID)
	{
		readBuff = new byte[1024];
		super(poll);
		_srcID = srcID;
		_dstID = dstID;
	}


	override bool doEstablished()
	{
		log_info(_srcID ," connected " , _dstID , " suc");
		return true;
	}

	void send(Message msg)
	{
		// if(msg.Type != MessageType.MsgHeartbeat && msg.Type != MessageType.MsgAppResp)
		// 	log_debug(_srcID , " sendto " , _dstID , " "  , msg);
		byte []data = serialize(msg);
		int len = cast(int)data.length;
		byte[4] head;
		memcpy(head.ptr , &len , 4);
		auto ret = doWrite(head , null , null);
		// if(msg.Type != MessageType.MsgHeartbeat && msg.Type != MessageType.MsgAppResp)
		// 	log_info(_srcID,"  to  ",_dstID,"---head---ret : ",ret);
		ret = doWrite(data , null , null);
		// if(msg.Type != MessageType.MsgHeartbeat && msg.Type != MessageType.MsgAppResp)
		// 	log_info(_srcID,"  to  ",_dstID,"---data---ret : ",ret);
	}

private:
	ulong _srcID;
	ulong _dstID;

}

