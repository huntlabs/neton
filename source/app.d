


import server.NetonServer;
import zhang2018.common.Log;
import std.conv;
import std.getopt;
import std.exception;
import std.stdio;
import server.NetonConfig;


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
	load_log_conf("./config/log.conf" , args[0] ~ to!string(NetonConfig.instance.selfConf().id));

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
	NetonServer.instance.wait();

	return 0;
}




