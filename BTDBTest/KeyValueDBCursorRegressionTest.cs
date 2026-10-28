using System;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

public class KeyValueDBCursorRegressionTest
{
    public static TheoryData<string> AllKinds => new() { "InMemory", "BTree" };

    static IKeyValueDB Create(string kind) => kind switch
    {
        "InMemory" => new InMemoryKeyValueDB(),
        _ => new BTreeKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = new InMemoryFileCollection(), Compression = new NoCompressionStrategy(),
            CompactorScheduler = null
        })
    };

    static void Fill(IKeyValueDB db, params byte[][] keys)
    {
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        foreach (var key in keys) cursor.CreateOrUpdateKeyValue(key, key);
        tr.Commit();
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void ValueWrittenInCurrentTransactionIsNotCorrupted(string kind)
    {
        using var db = Create(kind);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        var value = new byte[100];
        Random.Shared.NextBytes(value);
        cursor.CreateOrUpdateKeyValue([1], value);
        Span<byte> buffer = default;
        Assert.Equal(value, cursor.GetValueSpan(ref buffer).ToArray());
        Assert.False(cursor.IsValueCorrupted());
        tr.Commit();
        using var tr2 = db.StartReadOnlyTransaction();
        using var cursor2 = tr2.CreateCursor();
        Assert.True(cursor2.FindFirstKey([]));
        Assert.False(cursor2.IsValueCorrupted());
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void FailedFindFirstKeyInvalidatesCursor(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1, 1], [3, 1]);
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.FindFirstKey([1]));
        Assert.False(cursor.FindFirstKey([2]));
        Assert.False(cursor.IsValid());
        Assert.Equal(-1, cursor.GetKeyIndex());
        Assert.True(cursor.FindFirstKey([3]));
        Assert.False(cursor.FindFirstKey([4]));
        Assert.False(cursor.IsValid());
        Assert.Equal(-1, cursor.GetKeyIndex());
        // Invalid cursor starts from first key
        Assert.True(cursor.FindNextKey([]));
        Assert.Equal([1, 1], cursor.GetKeySpan(new Span<byte>()).ToArray());
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void FailedFindNextOrPreviousKeyInvalidatesCursor(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1, 1], [1, 2], [3, 1]);
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.FindLastKey([1]));
        Assert.False(cursor.FindNextKey([1]));
        Assert.False(cursor.IsValid());
        // Stepping again from the invalid cursor starts at the first key with the prefix.
        Assert.True(cursor.FindNextKey([1]));
        Assert.Equal([1, 1], cursor.GetKeySpan(new Span<byte>()).ToArray());
        Assert.False(cursor.FindPreviousKey([1]));
        Assert.False(cursor.IsValid());
        Assert.True(cursor.FindPreviousKey([1]));
        Assert.Equal([1, 2], cursor.GetKeySpan(new Span<byte>()).ToArray());
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void FailedFindLastKeyInvalidatesCursor(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1, 1], [3, 1]);
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.FindLastKey([1]));
        Assert.False(cursor.FindLastKey([2]));
        Assert.False(cursor.IsValid());
        Assert.Equal(-1, cursor.GetKeyIndex());
        // Invalid cursor starts from last key
        Assert.True(cursor.FindPreviousKey([]));
        Assert.Equal([3, 1], cursor.GetKeySpan(new Span<byte>()).ToArray());
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void FindKeyIndexWithPrefixDoesNotLeavePrefix(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1, 1], [1, 2], [2, 1], [2, 2]);
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.FindKeyIndex([1], 1));
        Assert.Equal([1, 2], cursor.GetKeySpan(new Span<byte>()).ToArray());
        Assert.False(cursor.FindKeyIndex([1], 2));
        Assert.False(cursor.IsValid());
        Assert.True(cursor.FindKeyIndex([2], 0));
        Assert.Equal(2, cursor.GetKeyIndex());
        Assert.False(cursor.FindKeyIndex([2], -1));
        Assert.False(cursor.IsValid());
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void FindNotFoundInvalidatesCursor(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1, 1], [3, 1]);
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        Assert.Equal(FindResult.Exact, cursor.Find([1, 1], 0));
        Assert.Equal(FindResult.NotFound, cursor.Find([2, 0], 1));
        Assert.False(cursor.IsValid());
        Assert.Equal(-1, cursor.GetKeyIndex());
        Assert.Equal(FindResult.NotFound, cursor.Find([4, 0], 1));
        Assert.False(cursor.IsValid());
        Assert.Equal(FindResult.NotFound, cursor.Find([0, 0], 1));
        Assert.False(cursor.IsValid());
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void InvalidCursorFindPreviousAfterOtherCursorWriteStartsFromLastKey(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1], [3]);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        using var writer = tr.CreateCursor();
        Assert.False(cursor.IsValid());
        writer.CreateOrUpdateKeyValue([2], [2]);
        Assert.False(cursor.IsValid());
        Assert.True(cursor.FindPreviousKey([]));
        Assert.Equal([3], cursor.GetKeySpan(new Span<byte>()).ToArray());
        cursor.Invalidate();
        writer.CreateOrUpdateKeyValue([4], [4]);
        Assert.True(cursor.FindNextKey([]));
        Assert.Equal([1], cursor.GetKeySpan(new Span<byte>()).ToArray());
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void FastIterateAfterOtherCursorWriteContinuesFromCurrentKey(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1], [3], [5]);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        using var writer = tr.CreateCursor();
        Assert.True(cursor.FindFirstKey([3]));
        writer.CreateOrUpdateKeyValue([2], [2]);
        var seen = new System.Collections.Generic.List<(long, byte)>();
        Span<byte> buffer = default;
        cursor.FastIterate(ref buffer, (index, key) =>
        {
            seen.Add((index, key[0]));
            return false;
        });
        Assert.Equal([(2L, (byte)3), (3L, (byte)5)], seen);
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void FastIterateAfterCurrentKeyWasErasedContinuesFromNextKey(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1], [3], [5]);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.FindFirstKey([3]));
        cursor.EraseCurrent();
        var seen = new System.Collections.Generic.List<(long, byte)>();
        Span<byte> buffer = default;
        cursor.FastIterate(ref buffer, (index, key) =>
        {
            seen.Add((index, key[0]));
            return false;
        });
        Assert.Equal([(1L, (byte)5)], seen);
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void SetValueAfterOtherCursorWrite(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1], [3]);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        using var writer = tr.CreateCursor();
        Assert.True(cursor.FindFirstKey([3]));
        writer.CreateOrUpdateKeyValue([2], [2]);
        cursor.SetValue([42]);
        Assert.Equal(2, cursor.GetKeyIndex());
        Assert.True(writer.FindFirstKey([3]));
        Span<byte> buffer = default;
        Assert.Equal([42], writer.GetValueSpan(ref buffer).ToArray());
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void FastIterateAfterLastKeyWasErasedDoesNothing(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1], [3]);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        using var eraser = tr.CreateCursor();
        Assert.True(cursor.FindLastKey([]));
        Assert.True(eraser.FindLastKey([]));
        eraser.EraseCurrent();
        var calls = 0;
        Span<byte> buffer = default;
        cursor.FastIterate(ref buffer, (_, _) =>
        {
            calls++;
            return false;
        });
        Assert.Equal(0, calls);
        eraser.FastIterate(ref buffer, (_, _) =>
        {
            calls++;
            return false;
        });
        Assert.Equal(0, calls);
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void FastIterateAfterOtherCursorErasedCurrentKeyContinuesFromNextKey(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1], [3], [5]);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        using var eraser = tr.CreateCursor();
        Assert.True(cursor.FindFirstKey([3]));
        Assert.True(eraser.FindFirstKey([3]));
        eraser.EraseCurrent();
        var seen = new System.Collections.Generic.List<(long, byte)>();
        Span<byte> buffer = default;
        cursor.FastIterate(ref buffer, (index, key) =>
        {
            seen.Add((index, key[0]));
            return false;
        });
        Assert.Equal([(1L, (byte)5)], seen);
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void FastIterateAfterOnlyLastKeyWasErasedDoesNothing(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1], [3]);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.FindLastKey([]));
        cursor.EraseCurrent();
        var calls = 0;
        Span<byte> buffer = default;
        cursor.FastIterate(ref buffer, (_, _) =>
        {
            calls++;
            return false;
        });
        Assert.Equal(0, calls);
    }

    [Theory]
    [MemberData(nameof(AllKinds))]
    public void KeyHasPrefixAfterOtherCursorWrite(string kind)
    {
        using var db = Create(kind);
        Fill(db, [1, 1], [3, 3]);
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        using var writer = tr.CreateCursor();
        Assert.True(cursor.FindFirstKey([3]));
        writer.CreateOrUpdateKeyValue([2], [2]);
        Assert.True(cursor.KeyHasPrefix([3]));
        Assert.False(cursor.KeyHasPrefix([2]));
    }
}
