using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using Xunit;
using System.IO;
using Xunit.Abstractions;

namespace BTDBTest;

public abstract class KeyValueDBTestBase
{
    readonly ITestOutputHelper _testOutputHelper;

    [Fact]
    public void CreateEmptyDatabase()
    {
        using var fileCollection = new InMemoryFileCollection();
        using (NewKeyValueDB(fileCollection))
        {
        }
    }

    [Fact]
    public void EmptyTransaction()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        tr.Commit();
    }

    protected abstract IKeyValueDB NewKeyValueDB(IFileCollection fileCollection);

    protected abstract IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression,
        uint fileSplitSize = int.MaxValue);

    protected abstract IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression,
        uint fileSplitSize,
        ICompactorScheduler? compactorScheduler);

    protected abstract IKeyValueDB NewKeyValueDB(KeyValueDBOptions options);

    [Fact]
    public void EmptyWritingTransaction()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartWritingTransaction().Result;
        tr.Commit();
    }

    [Fact]
    public void FirstTransaction()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        Assert.True(tr.CreateOrUpdateKeyValue(_key1, ReadOnlySpan<byte>.Empty));
        tr.Commit();
    }

    [Fact]
    public void FirstTransactionIsNumber1()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        Assert.Equal(0, tr.GetTransactionNumber());
        Assert.True(tr.CreateOrUpdateKeyValue(_key1, ReadOnlySpan<byte>.Empty));
        Assert.Equal(1, tr.GetTransactionNumber());
        tr.Commit();
    }

    [Fact]
    public void CanIterateAllTransactions()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        using var tr2 = db.StartTransaction();
        Assert.Equal(db.Transactions().ToHashSet(), new HashSet<IKeyValueDBTransaction> { tr, tr2 });
        Assert.True(tr.CreatedTime <= tr2.CreatedTime);
    }

    [Fact]
    public void CanGetSizeOfPair()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        tr.CreateOrUpdateKeyValue(_key1, new byte[1]);
        var s = tr.GetStorageSizeOfCurrentKey();
        Assert.Equal((uint)_key1.Length, s.Key);
        Assert.Equal(1u, s.Value);
    }

    [Fact]
    public void ReadOnlyTransactionThrowsOnWriteAccess()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartReadOnlyTransaction();
        Assert.Throws<BTDBTransactionRetryException>(() => tr.CreateKey(new byte[1]));
    }

    [Fact]
    public void MoreComplexTransaction()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        Assert.True(tr.CreateOrUpdateKeyValue(_key1, ReadOnlySpan<byte>.Empty));
        Assert.False(tr.CreateOrUpdateKeyValue(_key1, ReadOnlySpan<byte>.Empty));
        Assert.Equal(FindResult.Previous, tr.Find(Key2, 0));
        Assert.True(tr.CreateOrUpdateKeyValue(Key2, ReadOnlySpan<byte>.Empty));
        Assert.Equal(FindResult.Exact, tr.Find(_key1, 0));
        Assert.Equal(FindResult.Exact, tr.Find(Key2, 0));
        Assert.Equal(FindResult.Previous, tr.Find(_key3, 0));
        Assert.Equal(FindResult.Next, tr.Find(ReadOnlySpan<byte>.Empty, 0));
        tr.Commit();
    }

    [Fact]
    public void CommitWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using (var tr1 = db.StartTransaction())
        {
            tr1.CreateKey(_key1);
            using (var tr2 = db.StartTransaction())
            {
                Assert.Equal(0, tr2.GetTransactionNumber());
                Assert.False(tr2.FindExactKey(_key1));
            }

            tr1.Commit();
        }

        using (var tr3 = db.StartTransaction())
        {
            Assert.Equal(1, tr3.GetTransactionNumber());
            Assert.True(tr3.FindExactKey(_key1));
        }
    }

    [Fact]
    public void CommitWithUlongWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using (var tr1 = db.StartTransaction())
        {
            Assert.Equal(0ul, tr1.GetCommitUlong());
            tr1.SetCommitUlong(42);
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            Assert.Equal(42ul, tr2.GetCommitUlong());
        }
    }

    [Fact]
    public void UlongsAreRemembered()
    {
        var snapshot = new MemoryStream();
        using (var fileCollection = new InMemoryFileCollection())
        {
            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr1 = db.StartTransaction())
                {
                    Assert.Equal(0ul, tr1.GetUlong(0));
                    tr1.SetUlong(0, 42);
                    tr1.Commit();
                }

                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(42ul, tr2.GetUlong(0));
                }
            }

            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(42ul, tr2.GetUlong(0));
                }
            }

            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(42ul, tr2.GetUlong(0));
                    KeyValueDBExportImporter.Export(tr2, snapshot);
                }
            }
        }

        snapshot.Position = 0;
        using (var fileCollection = new InMemoryFileCollection())
        {
            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(0ul, tr2.GetUlong(0));
                    KeyValueDBExportImporter.Import(tr2, snapshot);
                    Assert.Equal(42ul, tr2.GetUlong(0));
                }
            }
        }
    }

    [Fact]
    public void RollbackWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using (var tr1 = db.StartTransaction())
        {
            tr1.CreateKey(_key1);
            // Rollback because of missing commit
        }

        using (var tr2 = db.StartTransaction())
        {
            Assert.Equal(0, tr2.GetTransactionNumber());
            Assert.False(tr2.FindExactKey(_key1));
        }
    }

    [Fact]
    public void OnlyOneWritingTransactionPossible()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr1 = db.StartTransaction();
        tr1.CreateKey(_key1);
        using var tr2 = db.StartTransaction();
        Assert.False(tr2.FindExactKey(_key1));
        Assert.Throws<BTDBTransactionRetryException>(() => tr2.CreateKey(Key2));
    }

    [Fact]
    public void OnlyOneWritingTransactionPossible2()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        var tr1 = db.StartTransaction();
        tr1.CreateKey(_key1);
        using var tr2 = db.StartTransaction();
        tr1.Commit();
        tr1.Dispose();
        Assert.False(tr2.FindExactKey(_key1));
        Assert.Throws<BTDBTransactionRetryException>(() => tr2.CreateKey(Key2));
    }

    [Fact]
    public void TwoEmptyWriteTransactionsWithNestedWaiting()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        Task<IKeyValueDBTransaction> trOuter;
        using (var tr = db.StartWritingTransaction().Result)
        {
            trOuter = db.StartWritingTransaction().AsTask();
            tr.Commit();
        }

        using (var tr = trOuter.Result)
        {
            tr.Commit();
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(500)]
    [InlineData(1200000)]
    public void BiggerKey(int keyLength)
    {
        var key = new byte[keyLength];
        for (var i = 0; i < keyLength; i++) key[i] = (byte)i;
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using (var tr1 = db.StartTransaction())
        {
            tr1.CreateOrUpdateKeyValue(key, ReadOnlySpan<byte>.Empty);
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            Assert.True(tr2.FindExactKey(key));
            Assert.Equal(key.ToArray(), tr2.GetKey().ToArray());
        }
    }

    [Fact]
    public void TwoTransactions()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using (var tr1 = db.StartTransaction())
        {
            tr1.CreateKey(_key1);
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            tr2.CreateKey(Key2);
            Assert.True(tr2.FindExactKey(_key1));
            Assert.True(tr2.FindExactKey(Key2));
            Assert.False(tr2.FindExactKey(_key3));
            tr2.Commit();
        }

        using (var tr3 = db.StartTransaction())
        {
            Assert.True(tr3.FindExactKey(_key1));
            Assert.True(tr3.FindExactKey(Key2));
            Assert.False(tr3.FindExactKey(_key3));
        }
    }

    [Theory]
    [InlineData(1000)]
    public void MultipleTransactions(int transactionCount)
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        var key = new byte[2 + transactionCount * 10];
        for (var i = 0; i < transactionCount; i++)
        {
            key[0] = (byte)(i / 256);
            key[1] = (byte)(i % 256);
            using var tr1 = db.StartTransaction();
            tr1.CreateOrUpdateKeyValue(key.AsSpan(0, 2 + i * 10), ReadOnlySpan<byte>.Empty);
            if (i % 100 == 0 || i == transactionCount - 1)
            {
                for (var j = 0; j < i; j++)
                {
                    key[0] = (byte)(j / 256);
                    key[1] = (byte)(j % 256);
                    Assert.Equal(FindResult.Exact, tr1.Find(key.AsSpan(0, 2 + j * 10), 0));
                }
            }

            tr1.Commit();
        }
    }

    [Theory]
    [InlineData(1000)]
    public void MultipleTransactions2(int transactionCount)
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        var key = new byte[2 + transactionCount * 10];
        for (var i = 0; i < transactionCount; i++)
        {
            key[0] = (byte)((transactionCount - i) / 256);
            key[1] = (byte)((transactionCount - i) % 256);
            using var tr1 = db.StartTransaction();
            tr1.CreateOrUpdateKeyValue(key.AsSpan(0, 2 + i * 10), ReadOnlySpan<byte>.Empty);
            if (i % 100 == 0 || i == transactionCount - 1)
            {
                for (var j = 0; j < i; j++)
                {
                    key[0] = (byte)((transactionCount - j) / 256);
                    key[1] = (byte)((transactionCount - j) % 256);
                    Assert.Equal(FindResult.Exact, tr1.Find(key.AsSpan(0, 2 + j * 10), 0));
                }
            }

            tr1.Commit();
        }
    }

    [Fact]
    public void SimpleFindPreviousKeyWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using (var tr1 = db.StartTransaction())
        {
            tr1.CreateKey(_key1);
            tr1.CreateKey(Key2);
            tr1.CreateKey(_key3);
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            Assert.True(tr2.FindExactKey(_key3));
            Assert.True(tr2.FindPreviousKey(ReadOnlySpan<byte>.Empty));
            Assert.Equal(_key1, tr2.GetKey().ToArray());
            Assert.False(tr2.FindPreviousKey(ReadOnlySpan<byte>.Empty));
        }
    }

    [Fact]
    public void FindKeyWithPreferPreviousKeyWorks()
    {
        const int keyCount = 10000;
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using (var tr = db.StartTransaction())
        {
            var key = new byte[100];
            for (var i = 0; i < keyCount; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                tr.CreateKey(key);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            var key = new byte[101];
            for (var i = 0; i < keyCount; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                var findKeyResult = tr.Find(key, 0);
                var idx = tr.GetKeyIndex();
                Assert.Equal(FindResult.Previous, findKeyResult);
                Assert.Equal(i, idx);
            }
        }

        using (var tr = db.StartTransaction())
        {
            var key = new byte[99];
            for (var i = 0; i < keyCount; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                var findKeyResult = tr.Find(key, 0);
                if (i == 0)
                {
                    Assert.Equal(FindResult.Next, findKeyResult);
                    Assert.Equal(i, tr.GetKeyIndex());
                }
                else
                {
                    Assert.Equal(FindResult.Previous, findKeyResult);
                    Assert.Equal(i - 1, tr.GetKeyIndex());
                }
            }
        }
    }

    [Fact]
    public void SimpleFindNextKeyWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using (var tr1 = db.StartTransaction())
        {
            tr1.CreateKey(_key1);
            tr1.CreateKey(Key2);
            tr1.CreateKey(_key3);
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            Assert.True(tr2.FindExactKey(_key3));
            Assert.True(tr2.FindNextKey(ReadOnlySpan<byte>.Empty));
            Assert.Equal(Key2, tr2.GetKey().ToArray());
            Assert.False(tr2.FindNextKey(ReadOnlySpan<byte>.Empty));
        }
    }

    [Fact]
    public void AdvancedFindPreviousAndNextKeyWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        var key = new byte[2];
        const int keysCreated = 10000;
        using (var tr = db.StartTransaction())
        {
            for (var i = 0; i < keysCreated; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                tr.CreateKey(key);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            Assert.Equal(-1, tr.GetKeyIndex());
            tr.FindExactKey(key);
            Assert.Equal(keysCreated - 1, tr.GetKeyIndex());
            for (var i = 1; i < keysCreated; i++)
            {
                Assert.True(tr.FindPreviousKey(ReadOnlySpan<byte>.Empty));
                Assert.Equal(keysCreated - 1 - i, tr.GetKeyIndex());
            }

            Assert.False(tr.FindPreviousKey(ReadOnlySpan<byte>.Empty));
            Assert.Equal(-1, tr.GetKeyIndex());
            for (var i = 0; i < keysCreated; i++)
            {
                Assert.True(tr.FindNextKey(ReadOnlySpan<byte>.Empty));
                Assert.Equal(i, tr.GetKeyIndex());
            }

            Assert.False(tr.FindNextKey(ReadOnlySpan<byte>.Empty));
            Assert.Equal(-1, tr.GetKeyIndex());
        }
    }

    [Fact]
    public void SetKeyIndexWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        var key = new byte[2];
        const int keysCreated = 10000;
        using (var tr = db.StartTransaction())
        {
            for (var i = 0; i < keysCreated; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                tr.CreateKey(key);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            Assert.False(tr.SetKeyIndex(keysCreated));
            for (var i = 0; i < keysCreated; i += 5)
            {
                Assert.True(tr.SetKeyIndex(i));
                key = tr.GetKey().ToArray();
                Assert.Equal((byte)(i / 256), key[0]);
                Assert.Equal((byte)(i % 256), key[1]);
                Assert.Equal(i, tr.GetKeyIndex());
            }
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(256)]
    [InlineData(5000)]
    [InlineData(10000000)]
    public void CreateOrUpdateKeyValueWorks(int length)
    {
        var valueBuf = new byte[length];
        new Random(0).NextBytes(valueBuf);
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using (var tr1 = db.StartTransaction())
        {
            Assert.True(tr1.CreateOrUpdateKeyValue(_key1, valueBuf));
            Assert.False(tr1.CreateOrUpdateKeyValue(_key1, valueBuf));
            Assert.True(tr1.CreateOrUpdateKeyValue(Key2, valueBuf));
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            Assert.True(tr2.FindExactKey(_key1));
            var valueBuf2 = tr2.GetValue().ToArray();
            for (var i = 0; i < length; i++)
            {
                if (valueBuf[i] != valueBuf2[i])
                    Assert.Equal(valueBuf[i], valueBuf2[i]);
            }

            Assert.True(tr2.FindExactKey(Key2));
            valueBuf2 = tr2.GetValue().ToArray();
            for (var i = 0; i < length; i++)
            {
                if (valueBuf[i] != valueBuf2[i])
                    Assert.Equal(valueBuf[i], valueBuf2[i]);
            }
        }
    }

    [Fact]
    public void FindFirstKeyWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        Assert.False(tr.FindFirstKey(ReadOnlySpan<byte>.Empty));
        tr.CreateKey(_key1);
        tr.CreateKey(Key2);
        tr.CreateKey(_key3);
        Assert.True(tr.FindFirstKey(ReadOnlySpan<byte>.Empty));
        Assert.Equal(_key1, tr.GetKey().ToArray());
        tr.Commit();
    }

    [Fact]
    public void FindLastKeyWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        Assert.False(tr.FindLastKey(ReadOnlySpan<byte>.Empty));
        tr.CreateKey(_key1);
        tr.CreateKey(Key2);
        tr.CreateKey(_key3);
        Assert.True(tr.FindLastKey(ReadOnlySpan<byte>.Empty));
        Assert.Equal(Key2, tr.GetKey().ToArray());
        tr.Commit();
    }

    [Fact]
    public void SimplePrefixWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        tr.CreateKey(_key1);
        tr.CreateKey(Key2);
        tr.CreateKey(_key3);
        Assert.Equal(3, tr.GetKeyValueCount());
        Assert.Equal(2, tr.GetKeyValueCount(_key1.AsSpan(0, 3)));
        tr.FindFirstKey(_key1.AsSpan(0, 3));
        Assert.Equal(_key1.AsSpan(0, 3).ToArray(), tr.GetKey().ToArray());
        tr.FindLastKey(_key1.AsSpan(0, 3));
        Assert.Equal(_key3, tr.GetKey().ToArray());
        tr.Commit();
    }

    [Fact]
    public void PrefixWithFindNextKeyWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        tr.CreateKey(_key1);
        tr.CreateKey(Key2);
        Assert.True(tr.FindFirstKey(Key2.AsSpan(0, 1)));
        Assert.True(tr.FindNextKey(Key2.AsSpan(0, 1)));
        Assert.False(tr.FindNextKey(Key2.AsSpan(0, 1)));
        tr.Commit();
    }

    [Fact]
    public void PrefixWithFindPrevKeyWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        tr.CreateKey(_key1);
        tr.CreateKey(Key2);
        Assert.True(tr.FindFirstKey(Key2.AsSpan(0, 1)));
        Assert.False(tr.FindPreviousKey(Key2.AsSpan(0, 1)));
        tr.Commit();
    }

    [Fact]
    public void SimpleEraseCurrentWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        tr.CreateKey(_key1);
        tr.CreateKey(Key2);
        tr.CreateKey(_key3);
        tr.EraseCurrent();
        Assert.True(tr.FindFirstKey(ReadOnlySpan<byte>.Empty));
        Assert.Equal(_key1, tr.GetKey().ToArray());
        Assert.True(tr.FindNextKey(ReadOnlySpan<byte>.Empty));
        Assert.Equal(Key2, tr.GetKey().ToArray());
        Assert.False(tr.FindNextKey(ReadOnlySpan<byte>.Empty));
        Assert.Equal(2, tr.GetKeyValueCount());
    }

    [Fact]
    public void AdvancedEraseRangeWorks()
    {
        foreach (var range in EraseRangeSource())
            AdvancedEraseRangeWorksCore(range[0], range[1], range[2]);
    }

    void AdvancedEraseRangeWorksCore(int createKeys, int removeStart, int removeCount)
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        var key = new byte[2];
        using (var tr = db.StartTransaction())
        {
            for (var i = 0; i < createKeys; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                tr.CreateKey(key);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            tr.EraseRange(removeStart, removeStart + removeCount - 1);
            Assert.Equal(createKeys - removeCount, tr.GetKeyValueCount());
            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            Assert.Equal(createKeys - removeCount, tr.GetKeyValueCount());
            for (var i = 0; i < createKeys; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                if (i >= removeStart && i < removeStart + removeCount)
                {
                    Assert.False(tr.FindExactKey(key), $"{i} should be removed");
                }
                else
                {
                    Assert.True(tr.FindExactKey(key), $"{i} should be found");
                }
            }
        }
    }

    // ReSharper disable once MemberCanBePrivate.Global
    public static IEnumerable<int[]> EraseRangeSource()
    {
        yield return new[] { 1, 0, 1 };
        for (var i = 11; i < 1000; i += i)
        {
            yield return new[] { i, 0, 1 };
            yield return new[] { i, i - 1, 1 };
            yield return new[] { i, i / 2, 1 };
            yield return new[] { i, i / 2, i / 4 };
            yield return new[] { i, i / 4, 1 };
            yield return new[] { i, i / 4, i / 2 };
            yield return new[] { i, i - i / 2, i / 2 };
            yield return new[] { i, 0, i / 2 };
            yield return new[] { i, 3 * i / 4, 1 };
            yield return new[] { i, 0, i };
        }
    }

    [Fact]
    public void ALotOf5KbTransactionsWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        for (var i = 0; i < 5000; i++)
        {
            var key = new byte[5000];
            using var tr = db.StartTransaction();
            key[0] = (byte)(i / 256);
            key[1] = (byte)(i % 256);
            Assert.True(tr.CreateKey(key));
            tr.Commit();
        }
    }

    [Fact]
    public void SetKeyPrefixInOneTransaction()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        var key = new byte[5];
        var value = new byte[100];
        var rnd = new Random();
        using (var tr = db.StartTransaction())
        {
            for (byte i = 0; i < 100; i++)
            {
                key[0] = i;
                for (byte j = 0; j < 100; j++)
                {
                    key[4] = j;
                    rnd.NextBytes(value);
                    tr.CreateOrUpdateKeyValue(key, value);
                }
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            for (byte i = 0; i < 100; i++)
            {
                key[0] = i;
                Assert.Equal(100, tr.GetKeyValueCount(key.AsSpan(0, 4)));
            }
        }
    }

    [Fact]
    public void CompressibleValueLoad()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        using var tr = db.StartTransaction();
        tr.CreateOrUpdateKeyValue(_key1, new byte[1000]);
        Assert.Equal(new byte[1000], tr.GetValue().ToArray());
        tr.Commit();
    }

    [Fact]
    public void VeryLongKeys()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        var key = new byte[200000];
        var value = new byte[100];
        using (var tr = db.StartTransaction())
        {
            for (byte i = 0; i < 250; i++)
            {
                key[100000] = i;
                tr.CreateOrUpdateKeyValue(key, value);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            for (byte i = 0; i < 250; i++)
            {
                key[100000] = i;
                Assert.True(tr.FindExactKey(key));
                tr.EraseCurrent();
            }
        }
    }

    [Fact]
    public void StartWritingTransactionWorks()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection);
        var tr1 = db.StartWritingTransaction().Result;
        var tr2Task = db.StartWritingTransaction();
        var task = Task.Factory.StartNew(() =>
        {
            var tr2 = tr2Task.Result;
            Assert.True(tr2.FindExactKey(_key1));
            tr2.CreateKey(Key2);
            tr2.Commit();
            tr2.Dispose();
        });
        tr1.CreateKey(_key1);
        tr1.Commit();
        tr1.Dispose();
        task.Wait(1000);
        using var tr = db.StartTransaction();
        Assert.True(tr.FindExactKey(_key1));
        Assert.True(tr.FindExactKey(Key2));
    }

    [Fact]
    public void RepairsOnReopen()
    {
        using var fileCollection = new InMemoryFileCollection();
        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                tr.CreateKey(_key1);
                tr.Commit();
            }

            using (var tr = db.StartTransaction())
            {
                tr.CreateKey(Key2);
                tr.Commit();
            }

            using (var tr = db.StartTransaction())
            {
                tr.CreateKey(_key3);
                // rollback
            }

            using (var db2 = NewKeyValueDB(fileCollection))
            {
                using (var tr = db2.StartTransaction())
                {
                    Assert.True(tr.FindExactKey(_key1));
                    Assert.True(tr.FindExactKey(Key2));
                    Assert.False(tr.FindExactKey(_key3));
                }
            }
        }

        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                Assert.True(tr.FindExactKey(_key1));
                Assert.True(tr.FindExactKey(Key2));
                Assert.False(tr.FindExactKey(_key3));
            }
        }
    }

    [Fact]
    public void MoreComplexReopen()
    {
        using var fileCollection = new InMemoryFileCollection();
        using (var db = NewKeyValueDB(fileCollection))
        {
            for (var i = 0; i < 100; i++)
            {
                var key = new byte[100];
                using var tr = db.StartTransaction();
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                Assert.True(tr.CreateOrUpdateKeyValue(key, key));
                tr.Commit();
            }

            using (var tr = db.StartTransaction())
            {
                tr.SetKeyIndex(0);
                tr.EraseCurrent();
                tr.EraseRange(1, 3);
                tr.Commit();
            }
        }

        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                var key = new byte[100];
                key[1] = 1;
                Assert.True(tr.FindExactKey(key));
                tr.FindNextKey(ReadOnlySpan<byte>.Empty);
                Assert.Equal(5, tr.GetKey()[1]);
                Assert.Equal(96, tr.GetKeyValueCount());
            }
        }
    }

    [Fact]
    public void AddingContinueToSameFileAfterReopen()
    {
        using var fileCollection = new InMemoryFileCollection();
        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                tr.CreateOrUpdateKeyValue(_key1, _key1);
                tr.Commit();
            }
        }

        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                tr.CreateOrUpdateKeyValue(Key2, Key2);
                tr.Commit();
            }

            _testOutputHelper.WriteLine(db.CalcStats());
        }

        Assert.Equal(1u, fileCollection.GetCount()); // Log
    }

    [Fact]
    public void AddingContinueToNewFileAfterReopenWithCorruption()
    {
        using var fileCollection = new InMemoryFileCollection();
        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                tr.CreateOrUpdateKeyValue(_key1, _key1);
                tr.Commit();
            }
        }

        fileCollection.SimulateCorruptionBySetSize(20 + 16);
        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                Assert.Equal(0, tr.GetKeyValueCount());
                tr.CreateOrUpdateKeyValue(Key2, Key2);
                tr.Commit();
            }

            _testOutputHelper.WriteLine(db.CalcStats());
        }

        Assert.True(2 <= fileCollection.GetCount());
    }

    [Fact]
    public void AddingContinueToSameFileAfterReopenOfDBWith2TransactionLogFiles()
    {
        using var fileCollection = new InMemoryFileCollection();
        using (var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
        {
            using (var tr = db.StartTransaction())
            {
                tr.CreateOrUpdateKeyValue(_key1, new byte[1024]);
                tr.CreateOrUpdateKeyValue(Key2, new byte[10]);
                tr.Commit();
            }
        }

        Assert.Equal(2u, fileCollection.GetCount());
        using (var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
        {
            using (var tr = db.StartTransaction())
            {
                tr.CreateOrUpdateKeyValue(Key2, new byte[1024]);
                tr.CreateOrUpdateKeyValue(_key3, new byte[10]);
                tr.Commit();
            }
        }

        Assert.Equal(3u, fileCollection.GetCount());
        using (var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
        {
            using (var tr = db.StartTransaction())
            {
                tr.CreateOrUpdateKeyValue(Key2, Key2);
                tr.Commit();
            }
        }

        Assert.Equal(3u, fileCollection.GetCount());
    }

    [Fact]
    public void CompactionDoesNotRemoveStillUsedFiles()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024, null);
        using (var tr = db.StartTransaction())
        {
            tr.CreateOrUpdateKeyValue(_key1, new byte[1024]);
            tr.CreateOrUpdateKeyValue(Key2, new byte[10]);
            tr.Commit();
        }

        var longTr = db.StartTransaction();
        using (var tr = db.StartTransaction())
        {
            tr.FindExactKey(_key1);
            tr.EraseCurrent();
            tr.Commit();
        }

        db.Compact(new CancellationToken());
        Assert.Equal(3u, fileCollection.GetCount()); // 2 Logs, 1 KeyIndex
        longTr.Dispose();
        db.Compact(new CancellationToken());
        Assert.Equal(2u, fileCollection.GetCount()); // 1 Log, 1 KeyIndex
        using (var tr = db.StartTransaction())
        {
            tr.CreateOrUpdateKeyValue(_key3, new byte[10]);
            tr.Commit();
        }

        using (var db2 = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
        {
            using (var tr = db2.StartTransaction())
            {
                Assert.True(tr.FindExactKey(_key3));
            }
        }
    }

    [Fact]
    public void CompactionStabilizedEvenWithOldTransactions()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 10240, null);
        using (var tr = db.StartTransaction())
        {
            tr.CreateOrUpdateKeyValue(_key1, new byte[4000]);
            tr.CreateOrUpdateKeyValue(Key2, new byte[4000]);
            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            tr.CreateOrUpdateKeyValue(_key3, new byte[4000]); // creates new Log
            tr.FindExactKey(_key1);
            tr.EraseCurrent();
            tr.Commit();
        }

        var longTr = db.StartTransaction();
        db.Compact(new CancellationToken());
        Assert.Equal(4u, fileCollection.GetCount()); // 2 Logs, 1 values, 1 KeyIndex
        db.Compact(new CancellationToken());
        Assert.Equal(4u, fileCollection.GetCount()); // 2 Logs, 1 values, 1 KeyIndex
        longTr.Dispose();
        db.Compact(new CancellationToken());
        Assert.Equal(3u, fileCollection.GetCount()); // 1 Log, 1 values, 1 KeyIndex
    }

    [Fact]
    public void PreapprovedCommitAndCompaction()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024);
        using (var tr = db.StartWritingTransaction().Result)
        {
            tr.CreateOrUpdateKeyValue(_key1, new byte[1024]);
            tr.CreateOrUpdateKeyValue(Key2, new byte[10]);
            tr.Commit();
        }

        db.Compact(new CancellationToken());
        using (var tr = db.StartWritingTransaction().Result)
        {
            tr.EraseRange(0, 0);
            tr.Commit();
        }

        db.Compact(new CancellationToken());
        using (var db2 = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
        {
            using (var tr = db2.StartTransaction())
            {
                Assert.False(tr.FindExactKey(_key1));
                Assert.True(tr.FindExactKey(Key2));
            }
        }
    }

    [Fact]
    public void AllowsToSetTransactionDescription()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024);
        using (var tr = db.StartTransaction())
        {
            Assert.Null(tr.DescriptionForLeaks);
            tr.DescriptionForLeaks = "Tr1";
            tr.CreateOrUpdateKeyValue(_key1, new byte[1]);
            Assert.Equal("Tr1", tr.DescriptionForLeaks);
            tr.Commit();
            Assert.Equal("Tr1", tr.DescriptionForLeaks);
        }

        using (var tr = db.StartTransaction())
        {
            Assert.Null(tr.DescriptionForLeaks);
        }
    }

    [Fact]
    public void ReportTransactionLeak()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024);
        var logger = new LoggerMock();
        db.Logger = logger;
        using (var tr = db.StartTransaction())
        {
            tr.CreateOrUpdateKeyValue(_key1, new byte[1]);
            tr.Commit();
        }

        Assert.Equal(fileCollection.GetCount(), logger.TrlCreatedCount);
        StartLeakingTransaction(db);
        GC.Collect(GC.MaxGeneration);
        GC.WaitForPendingFinalizers();
        Assert.NotNull(logger.Leaked);
        Assert.Equal("Leak", logger.Leaked.DescriptionForLeaks);
    }

    static void StartLeakingTransaction(IKeyValueDB db)
    {
        db.StartTransaction().DescriptionForLeaks = "Leak";
    }

    class LoggerMock : IKeyValueDBLogger
    {
        public IKeyValueDBTransaction? Leaked;
        public TimeSpan KviTime;
        public string? LastWarning;
        public uint TrlCreatedCount;
        public uint MarkedForDeleteCount;

        public void ReportTransactionLeak(IKeyValueDBTransaction transaction)
        {
            Leaked = transaction;
        }

        public void CompactionStart(ulong totalWaste)
        {
        }

        public void CompactionCreatedPureValueFile(uint fileId, ulong size, uint itemsInMap, ulong roughMemory)
        {
        }

        public void KeyValueIndexCreated(uint fileId, long keyValueCount, ulong size, TimeSpan duration)
        {
            KviTime = duration;
        }

        public void TransactionLogCreated(uint fileId)
        {
            TrlCreatedCount++;
        }

        public void FileMarkedForDelete(uint fileId)
        {
            MarkedForDeleteCount++;
        }

        public void LogWarning(string message)
        {
            LastWarning = message;
        }
    }

    [Fact]
    public void CompactionLimitsKviWriteSpeed()
    {
        using var fileCollection = new InMemoryFileCollection();
        var logger = new LoggerMock();
        using var db = NewKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = fileCollection,
            Compression = new NoCompressionStrategy(),
            CompactorScheduler = null,
            CompactorWriteBytesPerSecondLimit = 20000,
            FileSplitSize = 60000,
            Logger = logger
        });
        using (var tr = db.StartTransaction())
        {
            var key = new byte[100];
            for (var i = 0; i < 256; i++)
            {
                for (var j = 0; j < 100; j++) key[j] = (byte)i;
                tr.CreateOrUpdateKeyValue(key, key);
            }

            tr.Commit();
        }

        db.Compact(CancellationToken.None);
        // Kvi size = 27640 => ~1.4s
        Assert.InRange(logger.KviTime.TotalMilliseconds, 1000, 2000);
    }

    [Fact]
    public void BigCompaction()
    {
        using var fileCollection = new InMemoryFileCollection();
        var logger = new LoggerMock();
        using var db = NewKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = fileCollection,
            Compression = new NoCompressionStrategy(),
            CompactorScheduler = null,
            FileSplitSize = 10000,
            Logger = logger
        });
        using (var tr = db.StartTransaction())
        {
            var key = new byte[100];
            var value = new byte[2000];
            for (var i = 0; i < 2000; i++)
            {
                PackUnpack.PackInt32BE(key, 0, i);
                tr.CreateOrUpdateKeyValue(key, value);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            var key = new byte[100];
            for (var i = 0; i < 2000; i += 2)
            {
                PackUnpack.PackInt32BE(key, 0, i);
                tr.FindExactKey(key);
                tr.EraseCurrent();
            }

            for (var i = 0; i < 2000; i += 3)
            {
                PackUnpack.PackInt32BE(key, 0, i);
                if (tr.FindExactKey(key))
                    tr.EraseCurrent();
            }

            Assert.Equal(667, tr.GetKeyValueCount());
            tr.Commit();
        }

        db.Compact(CancellationToken.None);
        Assert.Equal(513u, logger.MarkedForDeleteCount);
    }

    [Fact]
    public void OpeningDbWithMissingFirstTrlAndKviWarnsAndOpenEmptyDb()
    {
        using var fileCollection = new InMemoryFileCollection();
        var options = new KeyValueDBOptions
        {
            FileCollection = fileCollection, Compression = new NoCompressionStrategy(), FileSplitSize = 1024,
            Logger = new LoggerMock()
        };
        Create2TrlFiles(options);
        fileCollection.GetFile(1)!.Remove();
        using var db = NewKeyValueDB(options);
        Assert.Equal("No valid Kvi and lowest Trl in chain is not first. Missing 1",
            ((LoggerMock)options.Logger).LastWarning);
        using var tr = db.StartTransaction();
        Assert.Equal(0, tr.GetKeyValueCount());
        Assert.Equal(0u, fileCollection.GetCount());
        tr.CreateOrUpdateKeyValue(_key1, new byte[1024]);
        tr.Commit();
    }

    [Fact]
    public void OpeningDbWithLenientOpenWithMissingFirstTrlAndKviWarnsAndRecoversData()
    {
        using var fileCollection = new InMemoryFileCollection();
        var options = new KeyValueDBOptions
        {
            FileCollection = fileCollection, Compression = new NoCompressionStrategy(), FileSplitSize = 1024,
            LenientOpen = true,
            Logger = new LoggerMock()
        };
        Create2TrlFiles(options);
        fileCollection.GetFile(1)!.Remove();
        using var db = NewKeyValueDB(options);
        Assert.Equal(
            "No valid Kvi and lowest Trl in chain is not first. Missing 1. LenientOpen is true, recovering data.",
            ((LoggerMock)options.Logger).LastWarning);
        using var tr = db.StartTransaction();
        Assert.Equal(1, tr.GetKeyValueCount());
        Assert.Equal(1u, fileCollection.GetCount());
    }

    void Create2TrlFiles(KeyValueDBOptions options)
    {
        using (var db = NewKeyValueDB(options))
        {
            using (var tr = db.StartTransaction())
            {
                tr.CreateOrUpdateKeyValue(_key1, new byte[1024]);
                tr.Commit();
            }

            using (var tr = db.StartTransaction())
            {
                tr.CreateOrUpdateKeyValue(_key3, new byte[50]);
                tr.Commit();
            }
        }
    }

    readonly byte[] _key1 = { 1, 2, 3 };

    // ReSharper disable once MemberCanBePrivate.Global
    public byte[] Key2 { get; } = { 1, 3, 2 };
    readonly byte[] _key3 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

    protected KeyValueDBTestBase(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }
}
