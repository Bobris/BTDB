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
        bool FindExact(in ReadOnlySpan<byte> key);
        bool FindFirst(in ReadOnlySpan<byte> keyPrefix);
        long FindLastWithPrefix(in ReadOnlySpan<byte> keyPrefix);
        FindResult Find(in ReadOnlySpan<byte> key);
        bool SeekIndex(long index);
        bool MoveNext();
        bool MovePrevious();
        long CalcIndex();
        long CalcDistance(ICursor to);
        bool IsValid();
        int GetKeyLength();
        Span<byte> FillByKey(in Span<byte> buffer);
        byte[] GetKeyAsByteArray();
        bool KeyHasPrefix(in ReadOnlySpan<byte> prefix);
        int GetValueLength();
        ReadOnlySpan<byte> GetValue();

        void WriteValue(in ReadOnlySpan<byte> content);
        bool Upsert(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> content);
        void Erase();
        long EraseTo(ICursor to);
        void BuildTree(long keyCount, ref SpanReader reader, BuildTreeCallback generator);
        void ValueReplacer(ref ValueReplacerCtx ctx);
    }

    public delegate void BuildTreeCallback(ref SpanReader reader, ref ByteBuffer key, in Span<byte> value);
}
