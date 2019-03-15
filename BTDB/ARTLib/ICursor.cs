using BTDB.KVDBLayer;
using System;

namespace BTDB.ARTLib
{
    public interface ICursor
    {
        void SetNewRoot(IRootNode artRoot);
        void Invalidate();
        ICursor Clone();
        bool FindExact(ReadOnlySpan<byte> key);
        bool FindFirst(ReadOnlySpan<byte> keyPrefix);
        bool FindLast(ReadOnlySpan<byte> keyPrefix);
        FindResult Find(ReadOnlySpan<byte> keyPrefix, ReadOnlySpan<byte> key);
        bool SeekIndex(long index);
        bool MoveNext();
        bool MovePrevious();
        long CalcIndex();
        long CalcDistance(ICursor to);
        bool IsValid();
        int GetKeyLength();
        Span<byte> FillByKey(Span<byte> buffer);
        bool KeyHasPrefix(ReadOnlySpan<byte> prefix);
        int GetValueLength();
        ReadOnlySpan<byte> GetValue();

        void WriteValue(ReadOnlySpan<byte> content);
        bool Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> content);
        void Erase();
        long EraseTo(ICursor to);
    }
}
