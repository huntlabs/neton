
import neton.server.NetonHttpServer;
import neton.server.NetonRpcServer;
import hunt.logging;
import core.thread;
import std.conv;
import std.getopt;
import std.exception;
import std.stdio;
import neton.server.NetonConfig;
import neton.rpcservice;
import grpc;
import hunt.util.DateTime;

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

    return true;
}


int main(string[] argv)
{
    import  std.parallelism; 
    defaultPoolThreads(100);
    // DateTime.startClock();
    bool join;
    if(!initConfig(argv,join))
    {
        throw new Exception("init config error !");
    }

    NetonRpcServer.instance.start(join);

    string host = "0.0.0.0";
    ushort port = NetonConfig.instance.selfConf().rpcport;

    Server server = new Server();
    server.listen(host , port);
    server.register( new KVService());
    server.register( new ConfigService());
    server.register( new RegistryService());
    server.register( new WatchService());
    server.register( new LeaseService());
    server.start();

    thread_joinAll();
    writeln("**************stop");
    return 0;
}




