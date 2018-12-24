module lease.LeaseQueue;

import lease.Lessor;

// LeaseWithTime contains lease object with a time.
// For the lessor's lease heap, time identifies the lease expiration time.
// For the lessor's lease checkpoint heap, the time identifies the next lease checkpoint time.
struct LeaseWithTime
{
	long id;
	// Unix nanos timestamp.
	long time;
	int index;
}

struct LeaseQueue
{
	private LeaseWithTime[] _queue;

	int Len()
	{
		return cast(int)(_queue.length);
	}

	bool Less(int i, int j)
	{
		return _queue[i].time < _queue[j].time;
	}

	void Swap(int i, int j)
	{
		auto temp = _queue[j];
		_queue[j] = _queue[i];
		_queue[i] = temp;
		_queue[i].index = i;
		_queue[j].index = j;
	}

	void Push(LeaseWithTime item)
	{
		auto n = _queue.length;
		item.index = cast(int)n;
		_queue ~= item;
	}

	LeaseWithTime Pop()
	{
		auto old = _queue.dup;
		auto n = old.length;
		auto item = old[n - 1];
		item.index = -1; // for safety
		_queue = old[0 .. n - 1];
		return item;
	}

	void clear()
	{
		_queue.length = 0;
	}

	LeaseWithTime get(int idx)
	{
		return _queue[idx];
	}
}
