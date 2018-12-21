module v3api.KVService;

import etcdserverpb.kv;
import etcdserverpb.rpc;
import etcdserverpb.rpcrpc;
import grpc;
import hunt.logging;

class KVService : KVBase
{

    override Status Range(RangeRequest , ref RangeResponse response){ 

        auto kv = new KeyValue();
        kv.key = cast(ubyte[])"key";
        kv.value = cast(ubyte[])"value";
        response.count = 1;
        response.kvs ~= kv;
        

        logInfo("range ");
        return Status.OK; 
    }
	override Status Put(PutRequest , ref PutResponse){ 
        return Status.OK; 
        }
	override Status DeleteRange(DeleteRangeRequest , ref DeleteRangeResponse){
         return Status.OK; 
         
         }

}