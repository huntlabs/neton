module network.server;

import hunt.net;
import network.base;
import hunt.logging;
import network.api;

class NetServer(T, A...)
{
    this(ulong ID, A args)
    {
        this.ID = ID;
        this.args = args;
        server = NetUtil.createNetServer!(ServerThreadMode.Single)();
    }

    void listen(string host, int port)
    {
        alias Server = hunt.net.Server.Server;
        server.listen(host, port, (Result!Server result) {
            if (result.failed())
                throw result.cause();
        });
        server.connectionHandler((NetSocket sock) {
            auto context = new T(sock, args);
            auto tcp = cast(AsynchronousTcpSession) sock;
            tcp.attachObject(context);
            logInfo(ID, " have a connection");
        });

    }

    A args;
    ulong ID;
    AbstractServer server;
}
