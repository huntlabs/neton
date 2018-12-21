


import server.NetonServer;
import hunt.logging;
import core.thread;
import std.conv;
import std.getopt;
import std.exception;
import std.stdio;
import server.NetonConfig;
import v3api.KVService;
import grpc;

bool initConfig(string[] args, out bool join)
{
	join = false;
	string confpath ;
	auto opt = getopt(args,"path|p","set config path",&confpath, "join|j","the server is join, default is false",&join);

	if (opt.helpWanted){
		defaultGetoptPrinter("Neton Server",
			opt.options);
		return false;
	}

	if (confpath.length <= 0)
	{
		throw new Exception("you must set config path!");
	}
	
	NetonConfig.instance.loadConf(confpath);

	LogConf conf;
	conf.fileName = "neton.log" ~ to!string(NetonConfig.instance.selfConf().id);
	logLoadConf(conf);
	// load_log_conf("./config/log.conf" , args[0] ~ to!string(NetonConfig.instance.selfConf().id));

	return true;
}

int main(string[] argv)
{
	bool join;
	if(!initConfig(argv,join))
	{
		throw new Exception("init config error !");
	}

    NetonServer.instance.start(join);

	string host = "0.0.0.0";
	ushort port = 50051;

	Server server = new Server();
	server.listen(host , port);
	server.register( new KVService());
	server.start();
	// NetonServer.instance.wait();
	// getchar();
	// while(1)
	// 	Thread.sleep(dur!"seconds"(1));
	thread_joinAll();
	writeln("**************stop");
	return 0;
}




