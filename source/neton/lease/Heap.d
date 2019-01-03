module neton.lease.Heap;

// Any type that implements heap.Interface may be used as a
// min-heap with the following invariants (established after
// Init has been called or if the data is empty or sorted):
//
//	!_queueLess(j, i) for 0 <= i < _queueLen() and 2*i+1 <= j <= 2*i+2 and j < _queueLen()
//
// Note that Push and Pop in this interface are for package heap's
// implementation to call. To add and remove things from the heap,
// use heap.Push and heap.Pop.

struct Heap(T)
{
	private T _queue;

	// A heap must be initialized before any of the heap operations
	// can be used. Init is idempotent with respect to the heap invariants
	// and may be called whenever the heap invariants may have been invalidated.
	// Its complexity is O(n) where n = _queueLen().
	//
	public void Init(T t)
	{
		// heapify
		_queue = t;
		auto n = t.Len();
		for (int i = n / 2 - 1; i >= 0; i--)
		{
			down(i, n);
		}
	}

	// Push pushes the element x onto the heap. The complexity is
	// O(log(n)) where n = _queueLen().
	//
	public void Push(D)(D d)
	{
		_queue.Push(d);
		up(_queue.Len() - 1);
	}

	// Pop removes the minimum element (according to Less) from the heap
	// and returns it. The complexity is O(log(n)) where n = _queueLen().
	// It is equivalent to Remove(h, 0).
	//
	public D Pop(D)()
	{
		auto n = _queue.Len() - 1;
		_queue.Swap(0, n);
		down(0, n);
		return _queue.Pop();
	}

	// Remove removes the element at index i from the heap.
	// The complexity is O(log(n)) where n = _queueLen().
	//
	public D Remove(D)(int i)
	{
		auto n = _queueLen() - 1;
		if (n != i)
		{
			_queue.Swap(i, n);
			if (!down(i, n))
			{
				up(i);
			}
		}
		return _queue.Pop();
	}

	public D get(D)(int idx)
	{
		return _queue.get(idx);
	}

	public void clear()
	{
		_queue.clear();
	}
	// Fix re-establishes the heap ordering after the element at index i has changed its value.
	// Changing the value of the element at index i and then calling Fix is equivalent to,
	// but less expensive than, calling Remove(h, i) followed by a Push of the new value.
	// The complexity is O(log(n)) where n = _queueLen().
	public void Fix(int i)
	{
		if (!down(i, _queue.Len()))
		{
			up(i);
		}
	}

	public int length()
	{
		return _queue.Len();
	}

	private void up(int j)
	{
		while (1)
		{
			auto i = (j - 1) / 2; // parent
			if (i == j || !_queue.Less(j, i))
			{
				break;
			}
			_queue.Swap(i, j);
			j = i;
		}
	}

	private bool down(int i0, int n)
	{
		auto i = i0;
		while (1)
		{
			auto j1 = 2 * i + 1;
			if (j1 >= n || j1 < 0)
			{ // j1 < 0 after int overflow
				break;
			}
			auto j = j1; // left child
			auto j2 = j1 + 1;
			if (j2 < n && _queue.Less(j2, j1))
			{
				j = j2; // = 2*i + 2  // right child
			}
			if (!_queue.Less(j, i))
			{
				break;
			}
			_queue.Swap(i, j);
			i = j;
		}
		return i > i0;
	}

}

unittest
{
	import neton.lease.Heap;
	import neton.lease.LeaseQueue;

	Heap!LeaseQueue queue;
	for (int i = 0; i < 10; i++)
	{
		LeaseWithTime item = {id:
		i, time : 10 - i};
		queue.Push(item);
	}

	Heap!LeaseQueue queue2 = queue;
	LeaseWithTime item = {id:
	12, time : 4};
	queue2.Push(item);
	while (queue2.lenght != 0)
	{
		writeln(" pop : ", queue2.Pop!LeaseWithTime());
	}
}
