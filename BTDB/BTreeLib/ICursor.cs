using BTDB.Buffer;
using BTDB.KVDBLayer;
using System;
using BTDB.StreamLayer;

namespace BTDB.BTreeLib
{
    public interface ICursor
    {
        void SetNewRoot(IRootNode btreeRoot);
        void Invalidate();
        ICursor Clone();
        bool FindExact(ReadOnlySpan<byte> key);
        bool FindFirst(ReadOnlySpan<byte> keyPrefix);
        long FindLastWithPrefix(ReadOnlySpan<byte> keyPrefix);
        FindResult Find(ReadOnlySpan<byte> key);
        FindResult Find(ReadOnlySpan<byte> keyPrefix, ReadOnlySpan<byte> key);
        bool SeekIndex(long index);
        bool MoveNext();
        bool MovePrevious();
        long CalcIndex();
        long CalcDistance(ICursor to);
        bool IsValid();
        int GetKeyLength();
        Span<byte> FillByKey(Span<byte> buffer);
        byte[] GetKeyAsByteArray();
        bool KeyHasPrefix(ReadOnlySpan<byte> prefix);
        int GetValueLength();
        ReadOnlySpan<byte> GetValue();

        void WriteValue(ReadOnlySpan<byte> content);
        bool Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> content);
        void Erase();
        long EraseTo(ICursor to);
        void BuildTree(long keyCount, ref SpanReader reader, BuildTreeCallback generator);
        void ValueReplacer(ref ValueReplacerCtx ctx);
    }

    public delegate void BuildTreeCallback(ref SpanReader reader, ref ByteBuffer key, Span<byte> value);
}
