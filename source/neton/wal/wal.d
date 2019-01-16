module neton.wal.wal;

import neton.wal.record;
import neton.wal.util;
import neton.wal.decoder;
import neton.wal.encoder;
import hunt.util.Serialize;	
import std.stdio;
import hunt.logging;
import std.file;
import std.experimental.allocator;
import hunt.raft;
import std.path;

	// SegmentSizeBytes is the preallocated size of each wal segment file.
	// The actual size might be larger than this. In general, the default
	// value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
const long  SegmentSizeBytes= 64 * 1000 * 1000 ;// 64MB

	//plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "wal")

	enum ErrMetadataConflict = "wal: conflicting metadata found";
	enum ErrFileNotFound     = "wal: file not found";
	enum ErrCRCMismatch      = "wal: crc mismatch";
	enum ErrSnapshotMismatch = "wal: snapshot mismatch";
	enum ErrSnapshotNotFound = "wal: snapshot not found";
	//crcTable            = crc32.MakeTable(crc32.Castagnoli)


// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
class WAL  {
	 string  _dir;// the living directory of the underlay files

	// dirFile is a fd for the wal directory for syncing on Rename
	//dirFile *os.File

	byte[]  _metadata;          // metadata recorded at the head of each WAL
	HardState  _hdstate  ; // hardstate recorded at the head of WAL

	WalSnapshot  _start;     // snapshot to start reading
	Decoder   _decoder;       // decoder to decode records

	ulong  _enti; // index of the last entry saved to the wal
	Encoder _encoder; // encoder to encode records


	File[] _fw; //(the name is increasing)
	File[] _fr; //(the name is increasing)

    //  creates a WAL ready for appending records. The given metadata is
    // recorded at the head of each WAL file, and can be retrieved with ReadAll.
		this(string dirpath,  byte[] metadata) {
			if (!isEmptyDir(dirpath)) {
				logError("wal dir not is empty...");
				return;
			}

			// keep temporary wal directory so WAL initialization appears atomic
			auto tmpdirpath = dirpath ~ ".tmp";
			if (Exist(tmpdirpath)) {
				rmdir(tmpdirpath);
			}
			mkdir(tmpdirpath);

			auto filepath = tmpdirpath ~ "/"~walName(0,0);
			auto fw = File(filepath,"wb+");
			if (!fw.isOpen()) {
				return;
			}
			_fw ~= fw;
			_dir = dirpath;
			_metadata = metadata;

			_encoder = theAllocator.make!Encoder(_fw[_fw.length-1]);
			
			if (_encoder is null) {
				logError("theAllocator make encoder object error...");
				return;
			}

			Record rec = {type:WalType.metadataType,data:cast(string)metadata};
			_encoder.encode(rec);
			
			WalSnapshot wsp;
			SaveSnapshot(wsp);
			
			
			renameDir(tmpdirpath,_dir);
	}

		this(string dirpath, WalSnapshot snap, bool write) {
			auto names =  readWalNames(dirpath);
			
			long idx = searchIndex(names, snap.index);
			if (idx < 0) {
				logError("search index ",snap.index);
				return ;
			}

			// open the wal files

			for(auto i = idx ; i< names.length;i++)
			{
				auto filepath = dirpath ~ "/" ~ names[idx];
				if(write)
				{
					auto fw = File(filepath,"ab+");
					if(fw.isOpen())
					{
						_fr ~= fw;
						_fw ~=fw;
					}
				}
				else
				{
					auto fr = File(filepath,"rb");
					if(fr.isOpen)
					{
						_fr ~= fr;
					}
				}
			}
			
			_dir = dirpath;
			_start = snap;
			_decoder = new Decoder(_fr);			
	}


	// ReadAll reads out records of the current WAL.
	// If opened in write mode, it must read out all records until EOF. Or an error
	// will be returned.
	// If opened in read mode, it will try to read all records if possible.
	// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
	// If loaded snap doesn't match with the expected one, it will return
	// all the records and error ErrSnapshotMismatch.
	// TODO: detect not-last-snap error.
	// TODO: maybe loose the checking of match.
	// After ReadAll, the WAL will be ready for appending new records.
	void ReadAll(out byte[] metadata, out HardState state, out Entry[] ents) {
		
		Record rec;

		bool match ;
		while (_decoder.decode(rec) != -1)
		{
			//logInfo("decoder --- --- ");	
			switch(rec.type) 
			{
			case WalType.entryType:
				Entry e = unserialize!Entry(cast(byte[])rec.data);
				if (e.Index > _start.index) {
					ents ~= e;
				}
				_enti = e.Index;
				break;
			case WalType.stateType:
				state = unserialize!HardState(cast(byte[])rec.data);
				break;
			case WalType.metadataType:
				metadata = cast(byte[])rec.data;
				break;
			case WalType.crcType:
				
				break;
			case WalType.snapshotType:
				WalSnapshot snap = unserialize!WalSnapshot(cast(byte[])rec.data);
				if (snap.index == _start.index) {
					if (snap.term != _start.term ){
						HardState st;
						state = st;
						logError("ErrSnapshotMismatch ");
						return ;
					}
					match = true;
				}
				break;
			default:
				HardState st;
				state = st;
				logError("unexpected block type ", rec.type);
				return;
			}
		}

		if (!match) {
			logError("ErrSnapshotNotFound ");
		}

		// close decoder, disable reading
		foreach(fp; _fr)
			fp.close();
		_fr.length = 0;

		WalSnapshot wsp;
		_start = wsp;

		_metadata = metadata;

		if(_fw.length > 0)
		{
			logInfo("make encoder ..before ");
			_encoder = theAllocator.make!Encoder(_fw[_fw.length-1]);
			logInfo("make encoder ..after ");
		}
		_decoder.Close();
		_decoder = null;
	}

	void saveEntry(Entry e) {
		// TODO: add MustMarshalTo to reduce one allocation.
		//logInfo("--------------debug 2-----");
		auto b = serialize(e);
		Record rec = {type: WalType.entryType, data:cast(string)b};
		if(_encoder !is null)
		{
			_encoder.encode(rec);
		}
		_enti = e.Index;
		return;
	}

	void saveState(HardState s) {
		//logInfo("--------------debug 1-----");
		if (IsEmptyHardState(s)) {
			return ;
		}
		_hdstate = s;
		auto b = serialize(s);
		Record rec = {type: WalType.stateType, data: cast(string)b};
		if(_encoder !is null)
		{
			_encoder.encode(rec);
		}
	}


	void Save(HardState st,Entry[] ents){

		// short cut, do not call sync
		if (IsEmptyHardState(st) && ents.length == 0 ){
			return ;
		}
		//logInfo("----- save hard state : ",st, " entry  : ",ents);

		bool mustSync = Ready.MustSync(st, _hdstate, cast(int)ents.length);

		// TODO(xiangli): no more reference operator
		foreach(e ; ents)  {
			saveEntry(e);
		}

		saveState(st);

		//logInfo("--------------debug -----");
		auto curOff = _encoder.curOff();

		if (curOff < SegmentSizeBytes) {
			//logInfo("----- curoff : ",curOff," mustSync : ",mustSync);
			if(mustSync)
				return sync();
			return;
		}

		return cut();

}


// cut closes current file written and creates a new one ready to append.
// cut first creates a temp wal file and writes necessary headers into it.
// Then cut atomically rename temp wal file to a wal file.
void cut()  {
		// close old wal file; truncate to avoid wasting space if an early cut
		logInfo("---- cut new file .");
		if(_fw.length > 0)
		{
			if(_fw[_fw.length - 1].isOpen)
				_fw[_fw.length - 1].flush();
		}

		auto fpath = _dir ~"/" ~ walName(seq()+1, _enti+1);

		// create a temp wal file with name sequence + 1, or truncate the existing one
		auto newtail = File(fpath,"wb+");
		if (!newtail.isOpen) {
			return;
		}

		// update writer and save the previous crc
		_fw ~= newtail;

		_encoder = theAllocator.make!Encoder(_fw[_fw.length-1]);
		if (_encoder is null) {
			return;
		}
		Record rec = {type:WalType.metadataType,data:cast(string)_metadata};

		_encoder.encode(rec);

		saveState(_hdstate);
		
		sync();

		_fw[_fw.length-1].seek(0,SEEK_SET);

		_encoder = theAllocator.make!Encoder(_fw[_fw.length-1]);
		if (_encoder is null) {
			return;
		}

		return;
}

	ulong seq()  {
		if(_fw.length <= 0)
			return 0;
	    auto basename = std.path.baseName(_fw[_fw.length-1].name());
		ulong seq,indx;
		parseWalName(basename,seq,indx);
		return seq;
}
	
	void SaveSnapshot(WalSnapshot sp)  {
		auto spdata = serialize(sp);

		Record rec = {type:WalType.snapshotType,data:cast(string)spdata};
		_encoder.encode(rec); 

		// update enti only when snapshot is ahead of last index
		if (_enti < sp.index) {
			_enti = sp.index;
		}
		return sync();
	}

	void sync()
	{
		if(_fw.length > 0)
		{
			if(_fw[_fw.length-1].isOpen)
				_fw[_fw.length-1].flush();
		}
		if(_encoder !is null)
			_encoder.flush();
	}

	void Close()
	{
		foreach(fp;_fw)
		{
		    logInfo("-----------close file name--------",fp.name);
			if(fp.isOpen)
				fp.flush();
			fp.close();
		}
		_fw.length = 0;
		foreach(fp;_fr)
		{
		    logInfo("-----------close file name--------",fp.name);
			if(fp.isOpen)
				fp.flush();
			fp.close();
		}
		_fr.length = 0;
	}
}

