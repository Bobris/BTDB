using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public abstract class KeyValueDBTestBase
{
    protected readonly ITestOutputHelper TestOutputHelper;

    [Fact]
    public void CreateEmptyDatabase()
    {
        using (NewKeyValueDB())
        {
        }
    }

    [Fact]
    public void EmptyTransaction()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        tr.Commit();
    }

    protected abstract IKeyValueDB NewKeyValueDB();

    [Fact]
    public void EmptyWritingTransaction()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartWritingTransaction().Result;
        tr.Commit();
    }

    [Fact]
    public void FirstTransaction()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        Assert.True(tr.CreateOrUpdateKeyValue(Key1, ReadOnlySpan<byte>.Empty));
        tr.Commit();
    }

    [Fact]
    public void FirstTransactionIsNumber1()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        Assert.Equal(0, tr.GetTransactionNumber());
        Assert.True(tr.CreateOrUpdateKeyValue(Key1, ReadOnlySpan<byte>.Empty));
        Assert.Equal(1, tr.GetTransactionNumber());
        tr.Commit();
    }

    [Fact]
    public void CanIterateAllTransactions()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var tr2 = db.StartTransaction();
        Assert.Equal(db.Transactions().ToHashSet(), new() { tr, tr2 });
        Assert.True(tr.CreatedTime <= tr2.CreatedTime);
    }

    [Fact]
    public void CanGetSizeOfPair()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        tr.CreateOrUpdateKeyValue(Key1, new byte[1]);
        var s = tr.GetStorageSizeOfCurrentKey();
        Assert.Equal((uint)Key1.Length, s.Key);
        Assert.Equal(1u, s.Value);
    }

    [Fact]
    public void ReadOnlyTransactionThrowsOnWriteAccess()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartReadOnlyTransaction();
        Assert.Throws<BTDBTransactionRetryException>(() => tr.CreateKey(new byte[1]));
    }

    [Fact]
    public void MoreComplexTransaction()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        Assert.True(tr.CreateOrUpdateKeyValue(Key1, ReadOnlySpan<byte>.Empty));
        Assert.False(tr.CreateOrUpdateKeyValue(Key1, ReadOnlySpan<byte>.Empty));
        Assert.Equal(FindResult.Previous, tr.Find(Key2, 0));
        Assert.True(tr.CreateOrUpdateKeyValue(Key2, ReadOnlySpan<byte>.Empty));
        Assert.Equal(FindResult.Exact, tr.Find(Key1, 0));
        Assert.Equal(FindResult.Exact, tr.Find(Key2, 0));
        Assert.Equal(FindResult.Previous, tr.Find(Key3, 0));
        Assert.Equal(FindResult.Next, tr.Find(ReadOnlySpan<byte>.Empty, 0));
        tr.Commit();
    }

    [Fact]
    public void CommitWorks()
    {
        using var db = NewKeyValueDB();
        using (var tr1 = db.StartTransaction())
        {
            tr1.CreateKey(Key1);
            using (var tr2 = db.StartTransaction())
            {
                Assert.Equal(0, tr2.GetTransactionNumber());
                Assert.False(tr2.FindExactKey(Key1));
            }

            tr1.Commit();
        }

        using (var tr3 = db.StartTransaction())
        {
            Assert.Equal(1, tr3.GetTransactionNumber());
            Assert.True(tr3.FindExactKey(Key1));
        }
    }

    [Fact]
    public void CommitWithUlongWorks()
    {
        using var db = NewKeyValueDB();
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
    public void RollbackWorks()
    {
        using var db = NewKeyValueDB();
        using (var tr1 = db.StartTransaction())
        {
            tr1.CreateKey(Key1);
            // Rollback because of missing commit
        }

        using (var tr2 = db.StartTransaction())
        {
            Assert.Equal(0, tr2.GetTransactionNumber());
            Assert.False(tr2.FindExactKey(Key1));
        }
    }

    [Fact]
    public void OnlyOneWritingTransactionPossible()
    {
        using var db = NewKeyValueDB();
        using var tr1 = db.StartTransaction();
        tr1.CreateKey(Key1);
        using var tr2 = db.StartTransaction();
        Assert.False(tr2.FindExactKey(Key1));
        Assert.Throws<BTDBTransactionRetryException>(() => tr2.CreateKey(Key2));
    }

    [Fact]
    public void OnlyOneWritingTransactionPossible2()
    {
        using var db = NewKeyValueDB();
        var tr1 = db.StartTransaction();
        tr1.CreateKey(Key1);
        using var tr2 = db.StartTransaction();
        tr1.Commit();
        tr1.Dispose();
        Assert.False(tr2.FindExactKey(Key1));
        Assert.Throws<BTDBTransactionRetryException>(() => tr2.CreateKey(Key2));
    }

    [Fact]
    public void TwoEmptyWriteTransactionsWithNestedWaiting()
    {
        using var db = NewKeyValueDB();
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
        using var db = NewKeyValueDB();
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
        using var db = NewKeyValueDB();
        using (var tr1 = db.StartTransaction())
        {
            tr1.CreateKey(Key1);
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            tr2.CreateKey(Key2);
            Assert.True(tr2.FindExactKey(Key1));
            Assert.True(tr2.FindExactKey(Key2));
            Assert.False(tr2.FindExactKey(Key3));
            tr2.Commit();
        }

        using (var tr3 = db.StartTransaction())
        {
            Assert.True(tr3.FindExactKey(Key1));
            Assert.True(tr3.FindExactKey(Key2));
            Assert.False(tr3.FindExactKey(Key3));
        }
    }

    [Theory]
    [InlineData(1000)]
    public void MultipleTransactions(int transactionCount)
    {
        using var db = NewKeyValueDB();
        var key = new byte[2 + transactionCount * 10];
        for (var i = 0; i < transactionCount; i++)
        {
            key[0] = (byte)(i / 256);
            key[1] = (byte)(i % 256);
            using var tr1 = db.StartTransaction();
            tr1.CreateOrUpdateKeyValue(key.AsSpan(0, 2 + i * 10), ReadOnlySpan<byte>.Empty);
            Assert.Equal(i, tr1.GetKeyIndex());
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
        using var db = NewKeyValueDB();
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
        using var db = NewKeyValueDB();
        using (var tr1 = db.StartTransaction())
        {
            tr1.CreateKey(Key1);
            tr1.CreateKey(Key2);
            tr1.CreateKey(Key3);
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            Assert.True(tr2.FindExactKey(Key3));
            Assert.True(tr2.FindPreviousKey(ReadOnlySpan<byte>.Empty));
            Assert.Equal(Key1, tr2.GetKey().ToArray());
            Assert.False(tr2.FindPreviousKey(ReadOnlySpan<byte>.Empty));
        }
    }

    [Fact]
    public void FindKeyWithPreferPreviousKeyWorks()
    {
        const int keyCount = 10000;
        using var db = NewKeyValueDB();
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
        using var db = NewKeyValueDB();
        using (var tr1 = db.StartTransaction())
        {
            tr1.CreateKey(Key1);
            tr1.CreateKey(Key2);
            tr1.CreateKey(Key3);
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            Assert.True(tr2.FindExactKey(Key3));
            Assert.True(tr2.FindNextKey(ReadOnlySpan<byte>.Empty));
            Assert.Equal(Key2, tr2.GetKey().ToArray());
            Assert.False(tr2.FindNextKey(ReadOnlySpan<byte>.Empty));
        }
    }

    [Fact]
    public void AdvancedFindPreviousAndNextKeyWorks()
    {
        using var db = NewKeyValueDB();
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
        using var db = NewKeyValueDB();
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
        using var db = NewKeyValueDB();
        using (var tr1 = db.StartTransaction())
        {
            Assert.True(tr1.CreateOrUpdateKeyValue(Key1, valueBuf));
            Assert.False(tr1.CreateOrUpdateKeyValue(Key1, valueBuf));
            Assert.True(tr1.CreateOrUpdateKeyValue(Key2, valueBuf));
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            Assert.True(tr2.FindExactKey(Key1));
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
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        Assert.False(tr.FindFirstKey(ReadOnlySpan<byte>.Empty));
        tr.CreateKey(Key1);
        tr.CreateKey(Key2);
        tr.CreateKey(Key3);
        Assert.True(tr.FindFirstKey(ReadOnlySpan<byte>.Empty));
        Assert.Equal(Key1, tr.GetKey().ToArray());
        tr.Commit();
    }

    [Fact]
    public void FindLastKeyWorks()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        Assert.False(tr.FindLastKey(ReadOnlySpan<byte>.Empty));
        tr.CreateKey(Key1);
        tr.CreateKey(Key2);
        tr.CreateKey(Key3);
        Assert.True(tr.FindLastKey(ReadOnlySpan<byte>.Empty));
        Assert.Equal(Key2, tr.GetKey().ToArray());
        tr.Commit();
    }

    [Fact]
    public void SimplePrefixWorks()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        tr.CreateKey(Key1);
        tr.CreateKey(Key2);
        tr.CreateKey(Key3);
        Assert.Equal(3, tr.GetKeyValueCount());
        Assert.Equal(2, tr.GetKeyValueCount(Key1.AsSpan(0, 3)));
        tr.FindFirstKey(Key1.AsSpan(0, 3));
        Assert.Equal(Key1.AsSpan(0, 3).ToArray(), tr.GetKey().ToArray());
        tr.FindLastKey(Key1.AsSpan(0, 3));
        Assert.Equal(Key3, tr.GetKey().ToArray());
        tr.Commit();
    }

    [Fact]
    public void PrefixWithFindNextKeyWorks()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        tr.CreateKey(Key1);
        tr.CreateKey(Key2);
        Assert.True(tr.FindFirstKey(Key2.AsSpan(0, 1)));
        Assert.True(tr.FindNextKey(Key2.AsSpan(0, 1)));
        Assert.False(tr.FindNextKey(Key2.AsSpan(0, 1)));
        tr.Commit();
    }

    [Fact]
    public void PrefixWithFindPrevKeyWorks()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        tr.CreateKey(Key1);
        tr.CreateKey(Key2);
        Assert.True(tr.FindFirstKey(Key2.AsSpan(0, 1)));
        Assert.False(tr.FindPreviousKey(Key2.AsSpan(0, 1)));
        tr.Commit();
    }

    [Fact]
    public void SimpleEraseCurrentWorks()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        tr.CreateKey(Key1);
        tr.CreateKey(Key2);
        tr.CreateKey(Key3);
        tr.EraseCurrent();
        Assert.True(tr.FindFirstKey(ReadOnlySpan<byte>.Empty));
        Assert.Equal(Key1, tr.GetKey().ToArray());
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
        using var db = NewKeyValueDB();
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
        using var db = NewKeyValueDB();
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
        using var db = NewKeyValueDB();
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
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        tr.CreateOrUpdateKeyValue(Key1, new byte[1000]);
        Assert.Equal(new byte[1000], tr.GetValue().ToArray());
        tr.Commit();
    }

    [Fact]
    public void VeryLongKeys()
    {
        using var db = NewKeyValueDB();
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
        using var db = NewKeyValueDB();
        var tr1 = db.StartWritingTransaction().Result;
        var tr2Task = db.StartWritingTransaction();
        var task = Task.Factory.StartNew(() =>
        {
            var tr2 = tr2Task.Result;
            Assert.True(tr2.FindExactKey(Key1));
            tr2.CreateKey(Key2);
            tr2.Commit();
            tr2.Dispose();
        });
        tr1.CreateKey(Key1);
        tr1.Commit();
        tr1.Dispose();
        task.Wait(1000);
        using var tr = db.StartTransaction();
        Assert.True(tr.FindExactKey(Key1));
        Assert.True(tr.FindExactKey(Key2));
    }

    [Fact]
    public void AllowsToSetTransactionDescription()
    {
        using var db = NewKeyValueDB();
        using (var tr = db.StartTransaction())
        {
            Assert.Null(tr.DescriptionForLeaks);
            tr.DescriptionForLeaks = "Tr1";
            tr.CreateOrUpdateKeyValue(Key1, new byte[1]);
            Assert.Equal("Tr1", tr.DescriptionForLeaks);
            tr.Commit();
            Assert.Equal("Tr1", tr.DescriptionForLeaks);
        }

        using (var tr = db.StartTransaction())
        {
            Assert.Null(tr.DescriptionForLeaks);
        }
    }

    protected readonly byte[] Key1 = { 1, 2, 3 };

    // ReSharper disable once MemberCanBePrivate.Global
    public byte[] Key2 { get; } = { 1, 3, 2 };
    protected readonly byte[] Key3 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

    protected KeyValueDBTestBase(ITestOutputHelper testOutputHelper)
    {
        TestOutputHelper = testOutputHelper;
    }
}
