using System.Collections.Generic;

namespace BTDB.ServiceLayer
{
    public class NumberAllocator
    {
        readonly object _lock = new object();
        readonly SortedSet<uint> _numbers = new SortedSet<uint>();
        uint _firstAllocatable;

        public NumberAllocator(uint firstAllocatable)
        {
            _firstAllocatable = firstAllocatable;
        }

        public uint Allocate()
        {
            lock (_lock)
            {
                if (_numbers.Count==0)
                {
                    return _firstAllocatable++;
                }
                var result = _numbers.Min;
                _numbers.Remove(result);
                return result;
            }
        }

        public void Deallocate(uint number)
        {
            lock (_lock)
            {
                _numbers.Add(number);
            }
        }
    }
}