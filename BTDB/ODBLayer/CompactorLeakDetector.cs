using System;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

sealed class CompactorLeakDetector
{
    readonly IObjectDBTransaction _transaction;
    readonly long _maxRememberedKeyBytes;
    MemWriter _keysToDelete;
    ulong _keyCountToDelete;
    readonly HashSet<uint> _leakedObjectTypeIds = [];
    LongBitArray? _usedKeys;
    long _startKeyIndex;
    long _keyCount;

    public CompactorLeakDetector(IObjectDBTransaction transaction, long maxRememberedKeyBytes)
    {
        _transaction = transaction;
        _maxRememberedKeyBytes = maxRememberedKeyBytes;
    }

    public void FindLeaks(CancellationToken cancellation)
    {
        LoadKeyRange(cancellation);
        if (_usedKeys == null)
            return;

        var iterator = new ODBIterator(_transaction, new Visitor(this));
        iterator.Iterate();
        CollectLeaks(cancellation);
        var keysToDelete = _keysToDelete.GetPersistentMemoryAndReset();
        KeysToDelete = keysToDelete;
        LeakedObjectTypeNames = ResolveLeakedObjectTypeNames();
    }

    public ReadOnlyMemory<byte> KeysToDelete { get; private set; }
    public ulong KeyCountToDelete => _keyCountToDelete;
    public IReadOnlyCollection<string> LeakedObjectTypeNames { get; private set; } = [];

    void LoadKeyRange(CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        using var cursor = _transaction.KeyValueDBTransaction.CreateCursor();
        if (!cursor.FindFirstKey(ObjectDB.AllObjectsPrefix) && !cursor.FindFirstKey(ObjectDB.AllDictionariesPrefix))
            return;
        var startIndex = cursor.GetKeyIndex();
        if (!cursor.FindLastKey(ObjectDB.AllDictionariesPrefix))
            cursor.FindLastKey(ObjectDB.AllObjectsPrefix);
        var count = cursor.GetKeyIndex() - startIndex + 1;
        _startKeyIndex = startIndex;
        _keyCount = count;
        _usedKeys = new(count);
    }

    void MarkCurrentKeyAsUsed(IKeyValueDBCursor cursor)
    {
        var index = cursor.GetKeyIndex();
        if (index < _startKeyIndex || index >= _startKeyIndex + _keyCount) return;
        _usedKeys!.Set(index - _startKeyIndex);
    }

    void CollectLeaks(CancellationToken cancellation)
    {
        using var cursor = _transaction.KeyValueDBTransaction.CreateCursor();
        var usedKeys = _usedKeys!;
        for (var nextIndex = 0L; usedKeys.TryGetNextUnset(nextIndex, out var index); nextIndex = index + 1)
        {
            cancellation.ThrowIfCancellationRequested();
            if (!cursor.FindKeyIndex(_startKeyIndex + index)) continue;
            if (!TryRememberLeak(cursor, cancellation)) return;
        }
    }

    [SkipLocalsInit]
    bool TryRememberLeak(IKeyValueDBCursor cursor, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        Span<byte> keyBuffer = stackalloc byte[4096];
        var key = cursor.GetKeySpan(ref keyBuffer);
        if (key.Length > 0 && key[0] == ObjectDB.AllObjectsPrefixByte)
            AddObjectTypeId(cursor);
        _keysToDelete.WriteVUInt32((uint)key.Length);
        _keysToDelete.WriteBlock(key);
        _keyCountToDelete++;
        return _keysToDelete.GetCurrentPosition() < _maxRememberedKeyBytes;
    }

    [SkipLocalsInit]
    void AddObjectTypeId(IKeyValueDBCursor cursor)
    {
        Span<byte> valueBuffer = stackalloc byte[4096];
        var value = cursor.GetValueSpan(ref valueBuffer);
        _leakedObjectTypeIds.Add((uint)PackUnpack.UnpackVUInt(value));
    }

    IReadOnlyCollection<string> ResolveLeakedObjectTypeNames()
    {
        if (_leakedObjectTypeIds.Count == 0) return [];

        var leakedObjectTypeNames = new HashSet<string>();
        foreach (var pair in ObjectDB.LoadTablesEnum(_transaction.KeyValueDBTransaction))
        {
            if (!_leakedObjectTypeIds.Remove(pair.Key)) continue;
            leakedObjectTypeNames.Add(pair.Value);
            if (_leakedObjectTypeIds.Count == 0) return leakedObjectTypeNames;
        }

        foreach (var tableId in _leakedObjectTypeIds)
            leakedObjectTypeNames.Add("UnknownTypeId:" + tableId);
        return leakedObjectTypeNames;
    }

    sealed class Visitor : IODBFastVisitor
    {
        readonly CompactorLeakDetector _detector;

        public Visitor(CompactorLeakDetector detector)
        {
            _detector = detector;
        }

        public void MarkCurrentKeyAsUsed(IKeyValueDBCursor cursor)
        {
            _detector.MarkCurrentKeyAsUsed(cursor);
        }
    }

    sealed class LongBitArray
    {
        const int BitsPerChunk = 1 << 26;
        readonly ulong[][] _chunks;
        readonly long _length;

        public LongBitArray(long length)
        {
            _length = length;
            var chunkCount = (length + BitsPerChunk - 1) / BitsPerChunk;
            _chunks = new ulong[checked((int)chunkCount)][];
            for (var i = 0; i < _chunks.Length; i++)
            {
                var remainingBits = length - (long)i * BitsPerChunk;
                var bitsInChunk = Math.Min(remainingBits, BitsPerChunk);
                _chunks[i] = new ulong[(int)((bitsInChunk + 63) / 64)];
            }
        }

        public void Set(long index)
        {
            var chunk = (int)(index / BitsPerChunk);
            var bitInChunk = index - chunk * BitsPerChunk;
            _chunks[chunk][(int)(bitInChunk / 64)] |= 1UL << (int)(bitInChunk & 63);
        }

        public bool TryGetNextUnset(long startIndex, out long index)
        {
            if (startIndex >= _length)
            {
                index = 0;
                return false;
            }

            var chunk = (int)(startIndex / BitsPerChunk);
            var bitInChunk = startIndex - (long)chunk * BitsPerChunk;
            var wordIndex = (int)(bitInChunk / 64);
            var bitInWord = (int)(bitInChunk & 63);
            while (chunk < _chunks.Length)
            {
                var words = _chunks[chunk];
                if (wordIndex < words.Length)
                {
                    var word = words[wordIndex] | ((1UL << bitInWord) - 1);
                    while (true)
                    {
                        if (word != ulong.MaxValue)
                        {
                            var candidate = (long)chunk * BitsPerChunk + (long)wordIndex * 64 +
                                            BitOperations.TrailingZeroCount(~word);
                            if (candidate < _length)
                            {
                                index = candidate;
                                return true;
                            }

                            index = 0;
                            return false;
                        }

                        wordIndex++;
                        if (wordIndex >= words.Length) break;
                        word = words[wordIndex];
                    }
                }

                chunk++;
                wordIndex = 0;
                bitInWord = 0;
            }

            index = 0;
            return false;
        }
    }
}
