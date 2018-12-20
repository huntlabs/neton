module v3api.KVService;

import etcdserverpb.kv;
import etcdserverpb.rpc;
import etcdserverpb.rpcrpc;
import grpc;

class KVService : KVBase
{

    override Status Range(RangeRequest , ref RangeResponse){ 
        return Status.OK; 
    }
	override Status Put(PutRequest , ref PutResponse){ 
        return Status.OK; 
        }
	override Status DeleteRange(DeleteRangeRequest , ref DeleteRangeResponse){
         return Status.OK; 
         
         }

}