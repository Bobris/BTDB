using System;
using System.Diagnostics;
using System.Threading;

namespace BTDB
{
    internal class Sector
    {
        internal SectorType Type { get; set; }

        internal long Position { get; set; }

        internal bool InTransaction { get; set; }

        internal bool Dirty { get; set; }

        internal bool Deleted { get; set; }

        internal int LastAccessTime { get; set; }

        internal Sector Parent { get; set; }

        internal Sector NextLink { get; set; }

        internal Sector PrevLink { get; set; }

        internal bool Locked
        {
            get { return _lockCount > 0; }
        }

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

        internal void Lock()
        {
            Interlocked.Increment(ref _lockCount);
            //Console.WriteLine("Lock {0} - {1} - {2} - {3}", Position, _lockCount, Type, Length);
        }

        internal void Unlock()
        {
            //Console.WriteLine("Unlock {0} - {1} - {2} - {3}", Position, _lockCount, Type, Length);
            Interlocked.Decrement(ref _lockCount);
            Debug.Assert(_lockCount >= 0);
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
                Debug.Assert(value >= 0 && value <= LowLevelDB.MaxSectorSize);
                Debug.Assert(value % LowLevelDB.AllocationGranularity == 0);
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
            Length = LowLevelDB.RoundToAllocationGranularity(length);
        }

        internal SectorPtr ToSectorPtr()
        {
            SectorPtr result;
            result.Ptr = Position;
            if (Type == SectorType.AllocChild)
            {
                if (Length < LowLevelDB.MaxLeafAllocSectorGrans / 8)
                    result.Ptr |= 255;
                else
                    result.Ptr |= BitArrayManipulation.SizeOfBiggestHoleUpTo255(Data);
            }
            else if (Type == SectorType.AllocParent)
            {
                if (Length < LowLevelDB.MaxChildren * LowLevelDB.PtrDownSize)
                    result.Ptr |= 255;
                else
                {
                    int res = 0;
                    for (int i = 0; i < LowLevelDB.MaxChildren; i++)
                    {
                        res = Math.Max(res, Data[i * LowLevelDB.PtrDownSize]);
                    }
                    result.Ptr |= res;
                }
            }
            else if (Position > 0)
            {
                result.Ptr |= Length / LowLevelDB.AllocationGranularity - 1;
            }
            result.Checksum = Dirty ? 0 : Checksum.CalcFletcher(Data, 0, (uint)Length);
            return result;
        }

        byte[] _data;
        int _lockCount;
    }
}