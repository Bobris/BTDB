using System.Collections.Generic;

namespace BTDB.KVDBLayer
{
    internal class FreeSpaceAllocatorOptimizer
    {
        readonly List<byte> _info = new List<byte>();

        internal void GlobalInvalidate()
        {
            _info.Clear();
            _info.TrimExcess();
        }

        internal void CommitWriteTransaction()
        {
            var count = _info.Count;
            for (int i = 0; i < count; i++)
            {
                var v = _info[i];
                if ((v & 128) != 0)
                {
                    _info[i] = 63;
                }
                else if ((v & 64) != 0)
                {
                    _info[i] = (byte) (v - 64);
                }
            }
        }

        internal void RollbackWriteTransaction()
        {
            var count = _info.Count;
            for (int i = 0; i < count; i++)
            {
                var v = _info[i];
                if ((v & 192) != 0)
                {
                    _info[i] = 63;
                }
            }
        }

        internal int QueryLongestForGran(ulong byteOffset)
        {
            var i = byteOffset / (KeyValueDB.MaxLeafAllocSectorGrans*KeyValueDB.AllocationGranularity);
            if (i >= (ulong)_info.Count) return int.MaxValue;
            var v = _info[(int)i] & 63;
            return v == 63 ? int.MaxValue : v;
        }

        internal void UpdateLongestForGran(ulong byteOffset, int len)
        {
            var i = byteOffset / (KeyValueDB.MaxLeafAllocSectorGrans * KeyValueDB.AllocationGranularity);
            if (i >= (ulong)_info.Count)
            {
                if (len > 62) return;
                while (i >= (ulong)_info.Count) _info.Add(128 + 63);
            }
            if (len > 63) len = 63;
            _info[(int)i] = (byte)((_info[(int)i] & 128) + 64 + len);
        }

        internal void InvalidateForNextTransaction(long startGran, int grans)
        {
            var i = startGran / KeyValueDB.MaxLeafAllocSectorGrans;
            var r = (int)(startGran % KeyValueDB.MaxLeafAllocSectorGrans)+grans;
            do
            {
                if (i >= _info.Count) return;
                _info[(int)i] |= 128;
                r -= KeyValueDB.MaxLeafAllocSectorGrans;
                i++;
            } while (r>0);
        }
    }
}
