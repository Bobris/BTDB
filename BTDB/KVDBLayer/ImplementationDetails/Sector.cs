using System;
using System.Diagnostics;
using System.Threading;
using BTDB.KVDBLayer.Helpers;
using BTDB.KVDBLayer.Implementation;

namespace BTDB.KVDBLayer.ImplementationDetails
{
    public sealed class Sector
    {
        internal SectorType Type { get; set; }

        internal long Position { get; set; }

        internal bool InTransaction { get; set; }

        internal bool Dirty { get; set; }

        internal bool Deleted { get; set; }

        internal ulong LastAccessTime { get; set; }

        internal long InternalLastAccessTime;

        internal bool InCache
        {
            set
            {
                if (value == _inCache) return;
                if (value)
                {
                    if (_parent != null)
                    {
                        Interlocked.Increment(ref _parent.ChildrenInCache);
                    }
                }
                else
                {
                    if (_parent != null)
                    {
                        Interlocked.Decrement(ref _parent.ChildrenInCache);
                    }
                }
                _inCache = value;
            }
        }
        internal int ChildrenInCache;

        Sector _parent;
        bool _inCache;

        internal Sector Parent
        {
            get { return _parent; }
            set
            {
                if (_inCache)
                {
                    if (_parent != null)
                    {
                        Interlocked.Decrement(ref _parent.ChildrenInCache);
                    }
                    _parent = value;
                    if (value != null)
                    {
                        Interlocked.Increment(ref value.ChildrenInCache);
                    }
                }
                else
                {
                    _parent = value;
                }
            }
        }

        internal Sector NextLink { get; set; }

        internal Sector PrevLink { get; set; }

        internal int Deepness
        {
            get
            {
                int result = 0;
                var t = this;
                while (t != null)
                {
                    result++;
                    t = t.Parent;
                }
                return result;
            }
        }

        internal bool Allocated
        {
            get { return Position > 0; }
        }

        internal byte[] Data
        {
            get { return _data; }
        }

        internal int Length
        {
            get
            {
                if (_data == null) return 0;
                return _data.Length;
            }

            set
            {
                Debug.Assert(value >= 0 && value <= KeyValueDB.MaxSectorSize);
                Debug.Assert(value % KeyValueDB.AllocationGranularity == 0);
                if (value == 0)
                {
                    _data = null;
                    return;
                }
                if (_data == null)
                {
                    _data = new byte[value];
                    return;
                }
                byte[] oldData = _data;
                _data = new byte[value];
                Array.Copy(oldData, _data, Math.Min(oldData.Length, value));
            }
        }

        internal void SetLengthWithRound(int length)
        {
            Length = KeyValueDB.RoundToAllocationGranularity(length);
        }

        internal SectorPtr ToSectorPtr()
        {
            SectorPtr result;
            result.Ptr = Position;
            if (Position > 0)
            {
                result.Ptr |= (uint)(Length / KeyValueDB.AllocationGranularity - 1);
            }
            result.Checksum = Dirty ? 0 : Checksum.CalcFletcher32(Data, 0, (uint)Length);
            return result;
        }

        byte[] _data;

    }
}