module neton.util.Queue;
import core.sync.mutex;

class Queue(T)
{
	private T[] _queue;
	private Mutex _mutex;
	
	this()
	{
		_mutex = new Mutex();
	}

	int length()
	{
		return cast(int)(_queue.length);
	}

	// bool less(int i, int j)
	// {
	// 	return _queue[i].time < _queue[j].time;
	// }

	void swap(int i, int j)
	{
		_mutex.lock();
		scope(exit) _mutex.unlock();

		auto temp = _queue[j];
		_queue[j] = _queue[i];
		_queue[i] = temp;
	}

	void push(T item)
	{
		_mutex.lock();
		scope(exit) _mutex.unlock();

		_queue ~= item;
	}

	T pop()
	{
		_mutex.lock();
		scope(exit) _mutex.unlock();

		auto old = _queue.dup;
		auto n = old.length;
		if(n == 0)
			return T.init;
		auto item = old[n - 1];
		_queue = old[0 .. n - 1];
		return item;
	}

	void clear()
	{
		_queue.length = 0;
	}

	T get(int idx)
	{
		if(idx >= length())
			return T.init;
		return _queue[idx];
	}
}
