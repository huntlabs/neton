module store.EventQueue;

import store.event;
import zhang2018.common.Log;

class EventQueue
{
    this(int capacity)
    {
        _capacity = capacity;
        _events = new Event[_capacity];
        _front = _back = _size = 0;
    }

    public :
        void insert(Event e)
        {
            try
            {
                 this._events[_back] = e;
                _back = (_back + 1) % _capacity;

                if (_size == _capacity) { //dequeue
                    _front = (_front + 1) % _capacity;
                } else {
                    _size++;
                } 
               log_info("event queue size : ", _size," front : ",_front," back : ",_back);
            }
            catch (Exception e)
            {
                log_error("catch %s", e.msg);
            }

        }

        ulong getIndex(int idx)
        {
            return _events[idx].Index();
            
        }

        @property int front()
        {
            return _front;
        }

        @property int back()
        {
            return _back;
        }

        @property int capacity()
        {
            return _capacity;
        }

        Event event(int i)
        {
            return _events[i];
        }
    private :
        int _size;
        int _front;
        int _back;
        int _capacity;
        Event[] _events;
}