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
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.CreateOrUpdateKeyValue(Key1, new()));
        tr.Commit();
    }

    [Fact]
    public void FirstTransactionIsNumber1()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        Assert.Equal(0, tr.GetTransactionNumber());
        Assert.True(cursor.CreateOrUpdateKeyValue(Key1, new()));
        Assert.Equal(1, tr.GetTransactionNumber());
        tr.Commit();
    }

    [Fact]
    public void CanIterateAllTransactions()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var tr2 = db.StartTransaction();
        Assert.Equal(db.Transactions().ToHashSet(), [tr, tr2]);
        Assert.True(tr.CreatedTime <= tr2.CreatedTime);
    }

    [Fact]
    public void CanGetSizeOfPair()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue(Key1, new byte[1]);
        var s = cursor.GetStorageSizeOfCurrentKey();
        Assert.Equal((uint)Key1.Length, s.Key);
        Assert.Equal(1u, s.Value);
    }

    [Fact]
    public void ReadOnlyTransactionThrowsOnWriteAccess()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        Assert.Throws<BTDBTransactionRetryException>(() => cursor.CreateKey(new byte[1]));
    }

    [Fact]
    public void MoreComplexTransaction()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.CreateOrUpdateKeyValue(Key1, new()));
        Assert.False(cursor.CreateOrUpdateKeyValue(Key1, new()));
        Assert.Equal(FindResult.Previous, cursor.Find(Key2, 0));
        Assert.True(cursor.CreateOrUpdateKeyValue(Key2, new()));
        Assert.Equal(FindResult.Exact, cursor.Find(Key1, 0));
        Assert.Equal(FindResult.Exact, cursor.Find(Key2, 0));
        Assert.Equal(FindResult.Previous, cursor.Find(Key3, 0));
        Assert.Equal(FindResult.Next, cursor.Find(new(), 0));
        tr.Commit();
    }

    [Fact]
    public void CommitWorks()
    {
        using var db = NewKeyValueDB();
        using (var tr1 = db.StartTransaction())
        {
            using var c1 = tr1.CreateCursor();
            c1.CreateKey(Key1);
            using (var tr2 = db.StartTransaction())
            {
                Assert.Equal(0, tr2.GetTransactionNumber());
                using var c2 = tr2.CreateCursor();
                Assert.False(c2.FindExactKey(Key1));
            }

            tr1.Commit();
        }

        using (var tr3 = db.StartTransaction())
        {
            Assert.Equal(1, tr3.GetTransactionNumber());
            using var c3 = tr3.CreateCursor();
            Assert.True(c3.FindExactKey(Key1));
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
            using var c1 = tr1.CreateCursor();
            c1.CreateKey(Key1);
            // Rollback because of missing commit
        }

        using (var tr2 = db.StartTransaction())
        {
            Assert.Equal(0, tr2.GetTransactionNumber());
            using var c2 = tr2.CreateCursor();
            Assert.False(c2.FindExactKey(Key1));
        }
    }

    [Fact]
    public void OnlyOneWritingTransactionPossible()
    {
        using var db = NewKeyValueDB();
        using var tr1 = db.StartTransaction();
        using var c1 = tr1.CreateCursor();
        c1.CreateKey(Key1);
        using var tr2 = db.StartTransaction();
        using var c2 = tr2.CreateCursor();
        Assert.False(c2.FindExactKey(Key1));
        Assert.Throws<BTDBTransactionRetryException>(() => c2.CreateKey(Key2));
    }

    [Fact]
    public void OnlyOneWritingTransactionPossible2()
    {
        using var db = NewKeyValueDB();
        var tr1 = db.StartTransaction();
        var c1 = tr1.CreateCursor();
        c1.CreateKey(Key1);
        using var tr2 = db.StartTransaction();
        c1.Dispose();
        tr1.Commit();
        tr1.Dispose();
        using var c2 = tr2.CreateCursor();
        Assert.False(c2.FindExactKey(Key1));
        Assert.Throws<BTDBTransactionRetryException>(() => c2.CreateKey(Key2));
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
            using (var c1 = tr1.CreateCursor())
            {
                c1.CreateOrUpdateKeyValue(key, new());
            }

            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            using (var c2 = tr2.CreateCursor())
            {
                Assert.True(c2.FindExactKey(key));
                var buf = new Span<byte>();
                Assert.Equal(key.AsSpan(), c2.GetKeySpan(ref buf));
            }
        }
    }

    [Fact]
    public void TwoTransactions()
    {
        using var db = NewKeyValueDB();
        using (var tr1 = db.StartTransaction())
        {
            using (var c1 = tr1.CreateCursor())
            {
                c1.CreateKey(Key1);
            }

            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            using var cursor = tr2.CreateCursor();
            cursor.CreateKey(Key2);
            Assert.True(cursor.FindExactKey(Key1));
            Assert.True(cursor.FindExactKey(Key2));
            Assert.False(cursor.FindExactKey(Key3));
            tr2.Commit();
        }

        using (var tr3 = db.StartTransaction())
        {
            using var cursor = tr3.CreateCursor();
            Assert.True(cursor.FindExactKey(Key1));
            Assert.True(cursor.FindExactKey(Key2));
            Assert.False(cursor.FindExactKey(Key3));
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
            using var cursor = tr1.CreateCursor();
            cursor.CreateOrUpdateKeyValue(key.AsSpan(0, 2 + i * 10), new());
            Assert.Equal(i, cursor.GetKeyIndex());
            if (i % 100 == 0 || i == transactionCount - 1)
            {
                for (var j = 0; j < i; j++)
                {
                    key[0] = (byte)(j / 256);
                    key[1] = (byte)(j % 256);
                    Assert.Equal(FindResult.Exact, cursor.Find(key.AsSpan(0, 2 + j * 10), 0));
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
            using var cursor = tr1.CreateCursor();
            cursor.CreateOrUpdateKeyValue(key.AsSpan(0, 2 + i * 10), new());
            if (i % 100 == 0 || i == transactionCount - 1)
            {
                for (var j = 0; j < i; j++)
                {
                    key[0] = (byte)((transactionCount - j) / 256);
                    key[1] = (byte)((transactionCount - j) % 256);
                    Assert.Equal(FindResult.Exact, cursor.Find(key.AsSpan(0, 2 + j * 10), 0));
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
            using var cursor = tr1.CreateCursor();
            cursor.CreateKey(Key1);
            cursor.CreateKey(Key2);
            cursor.CreateKey(Key3);
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            using var cursor = tr2.CreateCursor();
            Assert.True(cursor.FindExactKey(Key3));
            Assert.True(cursor.FindPreviousKey(new()));
            Assert.Equal(Key1, cursor.SlowGetKey());
            Assert.False(cursor.FindPreviousKey(new()));
        }
    }

    [Fact]
    public void FindKeyWithPreferPreviousKeyWorks()
    {
        const int keyCount = 10000;
        using var db = NewKeyValueDB();
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            var key = new byte[100];
            for (var i = 0; i < keyCount; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                cursor.CreateKey(key);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            var key = new byte[101];
            for (var i = 0; i < keyCount; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                var findKeyResult = cursor.Find(key, 0);
                var idx = cursor.GetKeyIndex();
                Assert.Equal(FindResult.Previous, findKeyResult);
                Assert.Equal(i, idx);
            }
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            var key = new byte[99];
            for (var i = 0; i < keyCount; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                var findKeyResult = cursor.Find(key, 0);
                if (i == 0)
                {
                    Assert.Equal(FindResult.Next, findKeyResult);
                    Assert.Equal(i, cursor.GetKeyIndex());
                }
                else
                {
                    Assert.Equal(FindResult.Previous, findKeyResult);
                    Assert.Equal(i - 1, cursor.GetKeyIndex());
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
            using var cursor = tr1.CreateCursor();
            cursor.CreateKey(Key1);
            cursor.CreateKey(Key2);
            cursor.CreateKey(Key3);
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            using var cursor = tr2.CreateCursor();
            Assert.True(cursor.FindExactKey(Key3));
            Assert.True(cursor.FindNextKey(new()));
            Assert.Equal(Key2, cursor.SlowGetKey());
            Assert.False(cursor.FindNextKey(new()));
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
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < keysCreated; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                cursor.CreateKey(key);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            Assert.Equal(-1, cursor.GetKeyIndex());
            cursor.FindExactKey(key);
            Assert.Equal(keysCreated - 1, cursor.GetKeyIndex());
            for (var i = 1; i < keysCreated; i++)
            {
                Assert.True(cursor.FindPreviousKey(new()));
                Assert.Equal(keysCreated - 1 - i, cursor.GetKeyIndex());
            }

            Assert.False(cursor.FindPreviousKey(new()));
            Assert.Equal(-1, cursor.GetKeyIndex());
            for (var i = 0; i < keysCreated; i++)
            {
                Assert.True(cursor.FindNextKey(new()));
                Assert.Equal(i, cursor.GetKeyIndex());
            }

            Assert.False(cursor.FindNextKey(new()));
            Assert.Equal(-1, cursor.GetKeyIndex());
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
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < keysCreated; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                cursor.CreateKey(key);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            Assert.False(cursor.FindKeyIndex(keysCreated));
            for (var i = 0; i < keysCreated; i += 5)
            {
                Assert.True(cursor.FindKeyIndex(i));
                key = cursor.SlowGetKey();
                Assert.Equal((byte)(i / 256), key[0]);
                Assert.Equal((byte)(i % 256), key[1]);
                Assert.Equal(i, cursor.GetKeyIndex());
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
            using var cursor = tr1.CreateCursor();
            Assert.True(cursor.CreateOrUpdateKeyValue(Key1, valueBuf));
            Assert.False(cursor.CreateOrUpdateKeyValue(Key1, valueBuf));
            Assert.True(cursor.CreateOrUpdateKeyValue(Key2, valueBuf));
            tr1.Commit();
        }

        using (var tr2 = db.StartTransaction())
        {
            using var cursor = tr2.CreateCursor();
            Assert.True(cursor.FindExactKey(Key1));
            var valueBuf2 = cursor.SlowGetValue();
            for (var i = 0; i < length; i++)
            {
                if (valueBuf[i] != valueBuf2[i])
                    Assert.Equal(valueBuf[i], valueBuf2[i]);
            }

            Assert.True(cursor.FindExactKey(Key2));
            valueBuf2 = cursor.SlowGetValue();
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
        using var cursor = tr.CreateCursor();
        Assert.False(cursor.FindFirstKey(new()));
        cursor.CreateKey(Key1);
        cursor.CreateKey(Key2);
        cursor.CreateKey(Key3);
        Assert.True(cursor.FindFirstKey(new()));
        Assert.Equal(Key1, cursor.SlowGetKey());
        tr.Commit();
    }

    [Fact]
    public void FindLastKeyWorks()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        Assert.False(cursor.FindLastKey(new()));
        cursor.CreateKey(Key1);
        cursor.CreateKey(Key2);
        cursor.CreateKey(Key3);
        Assert.True(cursor.FindLastKey(new()));
        Assert.Equal(Key2, cursor.SlowGetKey());
        tr.Commit();
    }

    [Fact]
    public void FindLastKeyReallyWorks()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        Assert.False(cursor.FindLastKey(new()));
        cursor.CreateKey([0, 1, 1, 1]);
        cursor.CreateKey([0, 2, 0]);
        Assert.False(cursor.FindLastKey([0, 1, 2]));
        Assert.True(cursor.FindLastKey([0, 2]));
        tr.Commit();
    }

    [Fact]
    public void SimplePrefixWorks()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateKey(Key1);
        cursor.CreateKey(Key2);
        cursor.CreateKey(Key3);
        Assert.Equal(3, tr.GetKeyValueCount());
        Assert.Equal(2, cursor.GetKeyValueCount(Key1.AsSpan(0, 3)));
        cursor.FindFirstKey(Key1.AsSpan(0, 3));
        Assert.Equal(Key1.AsSpan(0, 3).ToArray(), cursor.SlowGetKey());
        cursor.FindLastKey(Key1.AsSpan(0, 3));
        Assert.Equal(Key3, cursor.SlowGetKey());
        tr.Commit();
    }

    [Fact]
    public void PrefixWithFindNextKeyWorks()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateKey(Key1);
        cursor.CreateKey(Key2);
        Assert.True(cursor.FindFirstKey(Key2.AsSpan(0, 1)));
        Assert.True(cursor.FindNextKey(Key2.AsSpan(0, 1)));
        Assert.False(cursor.FindNextKey(Key2.AsSpan(0, 1)));
        tr.Commit();
    }

    [Fact]
    public void PrefixWithFindPrevKeyWorks()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateKey(Key1);
        cursor.CreateKey(Key2);
        Assert.True(cursor.FindFirstKey(Key2.AsSpan(0, 1)));
        Assert.False(cursor.FindPreviousKey(Key2.AsSpan(0, 1)));
        tr.Commit();
    }

    [Fact]
    public void SimpleEraseCurrentWorks()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateKey(Key1);
        cursor.CreateKey(Key2);
        cursor.CreateKey(Key3);
        cursor.EraseCurrent();
        Assert.True(cursor.FindFirstKey(new()));
        Assert.Equal(Key1, cursor.SlowGetKey());
        Assert.True(cursor.FindNextKey(new()));
        Assert.Equal(Key2, cursor.SlowGetKey());
        Assert.False(cursor.FindNextKey(new()));
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
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < createKeys; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                cursor.CreateKey(key);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.FindKeyIndex(removeStart);
            using var cursor2 = tr.CreateCursor();
            cursor2.FindKeyIndex(removeStart + removeCount - 1);
            Assert.Equal(removeCount, cursor.EraseUpTo(cursor2));
            Assert.Equal(createKeys - removeCount, tr.GetKeyValueCount());
            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            Assert.Equal(createKeys - removeCount, tr.GetKeyValueCount());
            for (var i = 0; i < createKeys; i++)
            {
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                if (i >= removeStart && i < removeStart + removeCount)
                {
                    Assert.False(cursor.FindExactKey(key), $"{i} should be removed");
                }
                else
                {
                    Assert.True(cursor.FindExactKey(key), $"{i} should be found");
                }
            }
        }
    }

    // ReSharper disable once MemberCanBePrivate.Global
    public static IEnumerable<int[]> EraseRangeSource()
    {
        yield return [1, 0, 1];
        for (var i = 11; i < 1000; i += i)
        {
            yield return [i, 0, 1];
            yield return [i, i - 1, 1];
            yield return [i, i / 2, 1];
            yield return [i, i / 2, i / 4];
            yield return [i, i / 4, 1];
            yield return [i, i / 4, i / 2];
            yield return [i, i - i / 2, i / 2];
            yield return [i, 0, i / 2];
            yield return [i, 3 * i / 4, 1];
            yield return [i, 0, i];
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
            using var cursor = tr.CreateCursor();
            key[0] = (byte)(i / 256);
            key[1] = (byte)(i % 256);
            Assert.True(cursor.CreateKey(key));
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
            using var cursor = tr.CreateCursor();
            for (byte i = 0; i < 100; i++)
            {
                key[0] = i;
                for (byte j = 0; j < 100; j++)
                {
                    key[4] = j;
                    rnd.NextBytes(value);
                    cursor.CreateOrUpdateKeyValue(key, value);
                }
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            for (byte i = 0; i < 100; i++)
            {
                key[0] = i;
                Assert.Equal(100, cursor.GetKeyValueCount(key.AsSpan(0, 4)));
            }
        }
    }

    [Fact]
    public void CompressibleValueLoad()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue(Key1, new byte[1000]);
        Assert.Equal(new byte[1000], cursor.SlowGetValue());
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
            using var cursor = tr.CreateCursor();
            for (byte i = 0; i < 250; i++)
            {
                key[100000] = i;
                cursor.CreateOrUpdateKeyValue(key, value);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            for (byte i = 0; i < 250; i++)
            {
                key[100000] = i;
                Assert.True(cursor.FindExactKey(key));
                cursor.EraseCurrent();
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
            {
                using var cursor = tr2.CreateCursor();
                Assert.True(cursor.FindExactKey(Key1));
                cursor.CreateKey(Key2);
            }
            tr2.Commit();
            tr2.Dispose();
        });
        {
            using var cursor = tr1.CreateCursor();
            cursor.CreateKey(Key1);
        }
        tr1.Commit();
        tr1.Dispose();
        task.Wait(1000);
        using var tr = db.StartTransaction();
        using var cursor2 = tr.CreateCursor();
        Assert.True(cursor2.FindExactKey(Key1));
        Assert.True(cursor2.FindExactKey(Key2));
    }

    [Fact]
    public void AllowsToSetTransactionDescription()
    {
        using var db = NewKeyValueDB();
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            Assert.Null(tr.DescriptionForLeaks);
            tr.DescriptionForLeaks = "Tr1";
            cursor.CreateOrUpdateKeyValue(Key1, new byte[1]);
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
    public void CanChangeKeySuffix()
    {
        using var db = NewKeyValueDB();
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key1, new byte[1]);
            Assert.Equal(UpdateKeySuffixResult.Updated, cursor.UpdateKeySuffix(Key3, 3));
            Assert.Equal(Key3, cursor.SlowGetKey());
            Assert.Equal(new byte[1], cursor.SlowGetValue());
            Assert.Equal(1, tr.GetKeyValueCount());
            Assert.Equal(0, cursor.GetKeyIndex());
            Assert.Equal(FindResult.Exact, cursor.Find(Key3, (uint)Key3.Length));
            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            Assert.Equal(FindResult.Exact, cursor.Find(Key3, (uint)Key3.Length));
            Assert.Equal(Key3, cursor.SlowGetKey());
            Assert.Equal(new byte[1], cursor.SlowGetValue());
            Assert.Equal(1, tr.GetKeyValueCount());
            Assert.Equal(0, cursor.GetKeyIndex());
            tr.Commit();
        }
    }

    [Fact]
    public void UpdateKeySuffixChecksForKeyPrefixUniqueness()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue(Key1, new byte[1]);
        cursor.CreateOrUpdateKeyValue(Key2, new byte[1]);
        Assert.Equal(UpdateKeySuffixResult.NotUniquePrefix, cursor.UpdateKeySuffix(Key3, 1));
    }

    [Fact]
    public void UpdateKeySuffixChecksForKeyModification()
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue(Key1, new byte[1]);
        cursor.CreateOrUpdateKeyValue(Key2, new byte[1]);
        Assert.Equal(UpdateKeySuffixResult.NothingToDo, cursor.UpdateKeySuffix(Key1, 2));
    }

    [Theory]
    [InlineData(100)]
    [InlineData(100000)]
    public void CanChangeKeySuffixInMany(int keyLength)
    {
        using var db = NewKeyValueDB();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        var key = new byte[keyLength];
        key[1] = 1;
        for (var i = 0; i < 250; i++)
        {
            key[10] = (byte)i;
            cursor.CreateOrUpdateKeyValue(key, new byte[i]);
        }

        key[keyLength - 1] = 1;
        for (var i = 0; i < 250; i++)
        {
            key[10] = (byte)i;
            Assert.Equal(UpdateKeySuffixResult.Updated, cursor.UpdateKeySuffix(key, (uint)keyLength / 2));
            Assert.True(key.AsSpan().SequenceEqual(cursor.GetKeySpan([])));
            Assert.Equal(i, cursor.SlowGetValue().Length);
            Assert.Equal(i, cursor.GetKeyIndex());
            Assert.Equal(250, tr.GetKeyValueCount());
        }

        key[10] = 250;
        Assert.Equal(UpdateKeySuffixResult.NotFound, cursor.UpdateKeySuffix(key, (uint)keyLength / 2));
        key[1] = 0;
        Assert.Equal(UpdateKeySuffixResult.NotFound, cursor.UpdateKeySuffix(key, (uint)keyLength / 2));
    }

    protected readonly byte[] Key1 = [1, 2, 3];

    // ReSharper disable once MemberCanBePrivate.Global
    public byte[] Key2 { get; } = [1, 3, 2];
    protected readonly byte[] Key3 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

    protected KeyValueDBTestBase(ITestOutputHelper testOutputHelper)
    {
        TestOutputHelper = testOutputHelper;
    }
}
