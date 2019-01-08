module neton.server.health;

import std.json;
import std.net.curl;
import std.stdio;
import core.time;
import std.parallelism;
import neton.store.event;

// import zhang2018.dreactor.time.Timer;
import hunt.event.timer;
import hunt.logging;
import neton.server.NetonHttpServer;
import neton.network.http;

class Health
{
    this(string key, JSONValue value)
    {
        _key = key;
        _value = value;
        parseValue();
    }

    void parseValue()
    {
        try
        {
            if (JSON_TYPE.OBJECT == _value.type)
            {
                if ("check" in _value)
                {
                    auto checkObj = _value["check"];
                    if (JSON_TYPE.OBJECT == checkObj.type)
                    {
                        if ("interval" in checkObj)
                        {
                            _interval = checkObj["interval"].integer;
                        }

                        if ("timeout" in checkObj)
                        {
                            _timeout = checkObj["timeout"].integer;
                        }

                        if ("http" in checkObj)
                        {
                            _http_url = checkObj["http"].str;
                        }
                    }
                }

                if ("status" in _value)
                {
                    _sState = cast(ServiceState)(_value["status"].str);
                }
            }
        }
        catch (Exception e)
        {

        }
    }

    void onTimer(AbstractTimer fd)
    {
        _timer = fd;
        //logWarning(_key,"  -- do health check.");
        if (_http_url.length > 0)
        {
            //logWarning(_key,"  -- do health check. url : ",_http_url," timeout : ",_timeout);
            taskPool.put(task!(makeCheck, Health)(this));
        }
    }

    ulong interval_ms()
    {
        return _interval * 1000;
    }

    @property AbstractTimer timerFd()
    {
        return _timer;
    }

    @property string http_url()
    {
        return _http_url;
    }

    @property long timeout()
    {
        return _timeout;
    }

    @property string key()
    {
        return _key;
    }

    @property ServiceState state()
    {
        return _sState;
    }

    @property void set_state(ServiceState st)
    {
        _sState = st;
    }

    @property JSONValue value()
    {
        return _value;
    }

private:
    string _key;
    JSONValue _value;
    ulong _interval = 10;
    long _timeout = 10;
    string _http_url;

    ServiceState _sState;
    AbstractTimer _timer;
}

void makeCheck(Health h)
{
    //logWarning(h.key,"  -- do health check. 2 url : ",h.http_url);
    try
    {
        auto http = HTTP(h.http_url);
        http.verifyHost = false;
        http.verifyPeer = false;
        http.operationTimeout = dur!"seconds"(h.timeout);
        http.onReceive = (ubyte[] data) { return data.length; };
        http.onReceiveStatusLine = (HTTP.StatusLine sl) {
            //logWarning(h.key,"  -- do health check. response code : ",sl.code);
            if (sl.code != 200)
                taskPool.put(task!(updateServiceState, Health,
                        ServiceState)(h, ServiceState.Critical));
            else
                taskPool.put(task!(updateServiceState, Health,
                        ServiceState)(h, ServiceState.Passing));

        };
        auto code = http.perform();
        //logWarning(h.key,"  -- do health check. perform code : ",code);
    }
    catch (Exception e)
    {
        logWarning(h.key, "  -- do health check. exception  : ", e.msg);
        taskPool.put(task!(updateServiceState, Health, ServiceState)(h, ServiceState.Critical));
    }
}

void updateServiceState(Health h, ServiceState state)
{
    try
    {
        //logWarning(h.key,"  -- update service state : ",state);
        if (h.state == state)
            return;
        h.set_state(state);
        auto val = h.value();
        val["status"] = state;

        RequestCommand command = {
        Method:
            RequestMethod.METHOD_UPDATESERVICE, Key : h.key, Hash : h.toHash(),
        Params : val.toString};
            NetonHttpServer.instance().Propose(command);
        }
        catch (Exception e)
        {
            logWarning(h.key, "  -- update service state exception  : ", e.msg);
        }
    }
