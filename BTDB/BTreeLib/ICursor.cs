using BTDB.Buffer;
using BTDB.KVDBLayer;
using System;
using BTDB.StreamLayer;

namespace BTDB.BTreeLib;

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
    bool IsValid();
    int GetKeyLength();
    ReadOnlySpan<byte> GetKey(scoped ref byte buffer, int bufferLength);
    bool KeyHasPrefix(in ReadOnlySpan<byte> prefix);
    int GetValueLength();
    ReadOnlySpan<byte> GetValue();

    void WriteValue(in ReadOnlySpan<byte> content);
    bool Upsert(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> content);
    void Erase();
    long EraseTo(ICursor to);
    void BuildTree(long keyCount, ref SpanReader reader, BuildTreeCallback generator);
    void ValueReplacer(ref ValueReplacerCtx ctx);

    byte[] GetKeyAsArray()
    {
        var res = new byte[GetKeyLength()];
        if (res.Length != 0) GetKey(ref res[0], res.Length);
        return res;
    }

    void TestTreeCorrectness();
}

public delegate void BuildTreeCallback(ref SpanReader reader, ref ByteBuffer key, in Span<byte> value);
