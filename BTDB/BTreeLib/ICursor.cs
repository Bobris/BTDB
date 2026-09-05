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
    ReadOnlySpan<byte> GetKeyParts(out ReadOnlySpan<byte> keySuffix);
    ReadOnlySpan<byte> GetKey(scoped ref byte buffer, int bufferLength);
    ReadOnlyMemory<byte> GetKeyMemory(ref Memory<byte> buffer, bool copy);
    ReadOnlySpan<byte> GetKeySpan(scoped ref Span<byte> buffer, bool copy);
    ReadOnlySpan<byte> GetKeySpan(Span<byte> buffer, bool copy);
    bool KeyHasPrefix(in ReadOnlySpan<byte> prefix);
    int GetValueLength();
    ReadOnlySpan<byte> GetValue();

    void WriteValue(in ReadOnlySpan<byte> content);
    bool Upsert(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> content);
    void UpdateKeySuffix(in ReadOnlySpan<byte> key);
    void Erase();
    long EraseTo(ICursor to);
    void BuildTree(long keyCount, ref MemReader reader, BuildTreeCallback generator);
    void ValueReplacer(ref ValueReplacerCtx ctx);

    byte[] GetKeyAsArray()
    {
        var res = new byte[GetKeyLength()];
        if (res.Length != 0) GetKey(ref res[0], res.Length);
        return res;
    }

    void TestTreeCorrectness();
    /// <summary>
    /// Iterates from the current position, maintaining cursor position and keyIndex for every callback.
    /// The callback may read the current key and value, but must not move or invalidate this cursor,
    /// reenter iteration, or modify the tree. The key span is valid only during the callback.
    /// </summary>
    void FastIterate(ref Span<byte> buffer, ref long keyIndex, CursorIterateCallback callback);

    /// <summary>
    /// Iterates keys from the current position. The callback must not use this cursor,
    /// access the keyIndex reference, or modify the tree. The key span is valid only during the callback.
    /// Cursor position and keyIndex are published when iteration returns or throws, and
    /// identify the current key when the callback returns true or throws.
    /// </summary>
    void FastIterateNoCursor(ref Span<byte> buffer, ref long keyIndex, CursorIterateCallback callback)
        => FastIterate(ref buffer, ref keyIndex, callback);
}

public delegate void BuildTreeCallback(ref MemReader reader, ref ByteBuffer key, in Span<byte> value);
