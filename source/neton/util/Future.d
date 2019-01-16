module neton.util.Future;
import core.sync.condition;
import core.sync.mutex;
import hunt.logging;
import hunt.util.Serialize;

class Future(Req , Res)
{

    Req _in;
    Res _out;
    Condition _condition;
    
    this(Req in_ )
    {
        _in = in_;
        // trace("**** key : ",cast(string)(_in.key));
        _condition = new Condition(new Mutex());
    }


    Res get()
    {
        _condition.mutex().lock();
        trace("condition waiting ...");
        _condition.wait(5.seconds);
        trace("condition waiting done ...");
        _condition.mutex().unlock();
        return _out;
    }

    void done(Res out_)
    {
        trace("condition notify ... ");
        _out = out_;
        _condition.notify();
    }

    Req data() @property
    {
        return _in;
    }

    ref Res ExtraData() @property
    {
        return _out;
    }

    void setExtraData(Res res)
    {
        _out = res;
    }
}


unittest
{
    import core.thread;
    import std.stdio;
    auto f = new Future!(string,string)("hello");
    new Thread((){
        Thread.sleep(dur!"secs"(1));
        if(f.data == "hello")
            f.done("world");
    }).start();
    writeln(f.get());
}