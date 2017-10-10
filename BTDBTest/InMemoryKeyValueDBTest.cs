using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest
{
    public class InMemoryInMemoryKeyValueDBTest
    {
        [Fact]
        public void CreateEmptyDatabase()
        {
            using (new InMemoryKeyValueDB())
            {
            }
        }

        [Fact]
        public void EmptyTransaction()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void EmptyWritingTransaction()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartWritingTransaction().Result)
                {
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void FirstTransaction()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void CanGetSizeOfPair()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[1]));
                    var s = tr.GetStorageSizeOfCurrentKey();
                    Assert.Equal(_key1.Length, (int)s.Key);
                    Assert.Equal(1u, s.Value);
                }
            }
        }

        [Fact]
        public void FirstTransactionIsNumber1()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.Equal(0, tr.GetTransactionNumber());
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    Assert.Equal(1, tr.GetTransactionNumber());
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void ReadOnlyTransactionThrowsOnWriteAccess()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartReadOnlyTransaction())
                {
                    Assert.Throws<BTDBTransactionRetryException>(() => tr.CreateKey(new byte[1]));
                }
            }
        }

        [Fact]
        public void MoreComplexTransaction()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    Assert.False(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    Assert.Equal(FindResult.Previous, tr.Find(ByteBuffer.NewAsync(_key2)));
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key2), ByteBuffer.NewAsync(new byte[0])));
                    Assert.Equal(FindResult.Exact, tr.Find(ByteBuffer.NewAsync(_key1)));
                    Assert.Equal(FindResult.Exact, tr.Find(ByteBuffer.NewAsync(_key2)));
                    Assert.Equal(FindResult.Previous, tr.Find(ByteBuffer.NewAsync(_key3)));
                    Assert.Equal(FindResult.Next, tr.Find(ByteBuffer.NewEmpty()));
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void CommitWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
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
        }

        [Fact]
        public void CommitWithUlongWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
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
        }

        [Fact]
        public void RollbackWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
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
        }

        [Fact]
        public void OnlyOneWrittingTransactionPossible()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    using (var tr2 = db.StartTransaction())
                    {
                        Assert.False(tr2.FindExactKey(_key1));
                        Assert.Throws<BTDBTransactionRetryException>(() => tr2.CreateKey(_key2));
                    }
                }
            }
        }

        [Fact]
        public void OnlyOneWrittingTransactionPossible2()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                var tr1 = db.StartTransaction();
                tr1.CreateKey(_key1);
                using (var tr2 = db.StartTransaction())
                {
                    tr1.Commit();
                    tr1.Dispose();
                    Assert.False(tr2.FindExactKey(_key1));
                    Assert.Throws<BTDBTransactionRetryException>(() => tr2.CreateKey(_key2));
                }
            }
        }

        [Fact]
        public void TwoEmptyWriteTransactionsWithNestedWaiting()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                Task<IKeyValueDBTransaction> trOuter;
                using (var tr = db.StartWritingTransaction().Result)
                {
                    trOuter = db.StartWritingTransaction();
                    tr.Commit();
                }
                using (var tr = trOuter.Result)
                {
                    tr.Commit();
                }
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(5000)]
        [InlineData(1200000)]
        public void BiggerKey(int keyLength)
        {
            var key = new byte[keyLength];
            for (int i = 0; i < keyLength; i++) key[i] = (byte)i;
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(key);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.True(tr2.FindExactKey(key));
                    Assert.Equal(key, tr2.GetKeyAsByteArray());
                }
            }
        }

        [Fact]
        public void TwoTransactions()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    tr2.CreateKey(_key2);
                    Assert.True(tr2.FindExactKey(_key1));
                    Assert.True(tr2.FindExactKey(_key2));
                    Assert.False(tr2.FindExactKey(_key3));
                    tr2.Commit();
                }
                using (var tr3 = db.StartTransaction())
                {
                    Assert.True(tr3.FindExactKey(_key1));
                    Assert.True(tr3.FindExactKey(_key2));
                    Assert.False(tr3.FindExactKey(_key3));
                }
            }
        }

        [Theory]
        [InlineData(1000)]
        public void MultipleTransactions(int transactionCount)
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                var key = new byte[2 + transactionCount * 10];
                for (int i = 0; i < transactionCount; i++)
                {
                    key[0] = (byte)(i / 256);
                    key[1] = (byte)(i % 256);
                    using (var tr1 = db.StartTransaction())
                    {
                        tr1.CreateOrUpdateKeyValue(ByteBuffer.NewSync(key, 0, 2 + i * 10), ByteBuffer.NewEmpty());
                        if (i % 100 == 0 || i == transactionCount - 1)
                        {
                            for (int j = 0; j < i; j++)
                            {
                                key[0] = (byte)(j / 256);
                                key[1] = (byte)(j % 256);
                                Assert.Equal(FindResult.Exact, tr1.Find(ByteBuffer.NewSync(key, 0, 2 + j * 10)));
                            }
                        }
                        tr1.Commit();
                    }
                }
            }
        }

        [Theory]
        [InlineData(1000)]
        public void MultipleTransactions2(int transactionCount)
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                var key = new byte[2 + transactionCount * 10];
                for (int i = 0; i < transactionCount; i++)
                {
                    key[0] = (byte)((transactionCount - i) / 256);
                    key[1] = (byte)((transactionCount - i) % 256);
                    using (var tr1 = db.StartTransaction())
                    {
                        tr1.CreateOrUpdateKeyValue(ByteBuffer.NewSync(key, 0, 2 + i * 10), ByteBuffer.NewEmpty());
                        if (i % 100 == 0 || i == transactionCount - 1)
                        {
                            for (int j = 0; j < i; j++)
                            {
                                key[0] = (byte)((transactionCount - j) / 256);
                                key[1] = (byte)((transactionCount - j) % 256);
                                Assert.Equal(FindResult.Exact, tr1.Find(ByteBuffer.NewSync(key, 0, 2 + j * 10)));
                            }
                        }
                        tr1.Commit();
                    }
                }
            }
        }

        [Fact]
        public void SimpleFindPreviousKeyWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    tr1.CreateKey(_key2);
                    tr1.CreateKey(_key3);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.True(tr2.FindExactKey(_key3));
                    Assert.True(tr2.FindPreviousKey());
                    Assert.Equal(_key1, tr2.GetKeyAsByteArray());
                    Assert.False(tr2.FindPreviousKey());
                }
            }
        }

        [Fact]
        public void FindKeyWithPreferPreviousKeyWorks()
        {
            const int keyCount = 10000;
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    var key = new byte[100];
                    for (int i = 0; i < keyCount; i++)
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
                    for (int i = 0; i < keyCount; i++)
                    {
                        key[0] = (byte)(i / 256);
                        key[1] = (byte)(i % 256);
                        var findKeyResult = tr.Find(ByteBuffer.NewSync(key));
                        Assert.Equal(FindResult.Previous, findKeyResult);
                        Assert.Equal(i, tr.GetKeyIndex());
                    }
                }
                using (var tr = db.StartTransaction())
                {
                    var key = new byte[99];
                    for (int i = 0; i < keyCount; i++)
                    {
                        key[0] = (byte)(i / 256);
                        key[1] = (byte)(i % 256);
                        var findKeyResult = tr.Find(ByteBuffer.NewSync(key));
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
        }

        [Fact]
        public void SimpleFindNextKeyWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    tr1.CreateKey(_key2);
                    tr1.CreateKey(_key3);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.True(tr2.FindExactKey(_key3));
                    Assert.True(tr2.FindNextKey());
                    Assert.Equal(_key2, tr2.GetKeyAsByteArray());
                    Assert.False(tr2.FindNextKey());
                }
            }
        }

        [Fact]
        public void AdvancedFindPreviousAndNextKeyWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                var key = new byte[2];
                const int keysCreated = 10000;
                using (var tr = db.StartTransaction())
                {
                    for (int i = 0; i < keysCreated; i++)
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
                    for (int i = 1; i < keysCreated; i++)
                    {
                        Assert.True(tr.FindPreviousKey());
                        Assert.Equal(keysCreated - 1 - i, tr.GetKeyIndex());
                    }
                    Assert.False(tr.FindPreviousKey());
                    Assert.Equal(-1, tr.GetKeyIndex());
                    for (int i = 0; i < keysCreated; i++)
                    {
                        Assert.True(tr.FindNextKey());
                        Assert.Equal(i, tr.GetKeyIndex());
                    }
                    Assert.False(tr.FindNextKey());
                    Assert.Equal(-1, tr.GetKeyIndex());
                }
            }
        }

        [Fact]
        public void SetKeyIndexWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                var key = new byte[2];
                const int keysCreated = 10000;
                using (var tr = db.StartTransaction())
                {
                    for (int i = 0; i < keysCreated; i++)
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
                    for (int i = 0; i < keysCreated; i += 5)
                    {
                        Assert.True(tr.SetKeyIndex(i));
                        key = tr.GetKeyAsByteArray();
                        Assert.Equal((byte)(i / 256), key[0]);
                        Assert.Equal((byte)(i % 256), key[1]);
                        Assert.Equal(i, tr.GetKeyIndex());
                    }
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
            var valbuf = new byte[length];
            new Random(0).NextBytes(valbuf);
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr1 = db.StartTransaction())
                {
                    Assert.True(tr1.CreateOrUpdateKeyValueUnsafe(_key1, valbuf));
                    Assert.False(tr1.CreateOrUpdateKeyValueUnsafe(_key1, valbuf));
                    Assert.True(tr1.CreateOrUpdateKeyValueUnsafe(_key2, valbuf));
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.True(tr2.FindExactKey(_key1));
                    var valbuf2 = tr2.GetValueAsByteArray();
                    for (int i = 0; i < length; i++)
                    {
                        if (valbuf[i] != valbuf2[i])
                            Assert.Equal(valbuf[i], valbuf2[i]);
                    }
                    Assert.True(tr2.FindExactKey(_key2));
                    valbuf2 = tr2.GetValueAsByteArray();
                    for (int i = 0; i < length; i++)
                    {
                        if (valbuf[i] != valbuf2[i])
                            Assert.Equal(valbuf[i], valbuf2[i]);
                    }
                }
            }
        }

        [Fact]
        public void FindFirstKeyWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.False(tr.FindFirstKey());
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.CreateKey(_key3);
                    Assert.True(tr.FindFirstKey());
                    Assert.Equal(_key1, tr.GetKeyAsByteArray());
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void FindLastKeyWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.False(tr.FindLastKey());
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.CreateKey(_key3);
                    Assert.True(tr.FindLastKey());
                    Assert.Equal(_key2, tr.GetKeyAsByteArray());
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void SimplePrefixWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.CreateKey(_key3);
                    Assert.Equal(3, tr.GetKeyValueCount());
                    tr.SetKeyPrefix(ByteBuffer.NewAsync(_key1, 0, 3));
                    Assert.Equal(2, tr.GetKeyValueCount());
                    tr.FindFirstKey();
                    Assert.Equal(new byte[0], tr.GetKeyAsByteArray());
                    tr.FindLastKey();
                    Assert.Equal(_key3.Skip(3).ToArray(), tr.GetKeyAsByteArray());
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void PrefixWithFindNextKeyWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.SetKeyPrefix(ByteBuffer.NewAsync(_key2, 0, 1));
                    Assert.True(tr.FindFirstKey());
                    Assert.True(tr.FindNextKey());
                    Assert.False(tr.FindNextKey());
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void PrefixWithFindPrevKeyWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.SetKeyPrefix(ByteBuffer.NewAsync(_key2, 0, 1));
                    Assert.True(tr.FindFirstKey());
                    Assert.False(tr.FindPreviousKey());
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void SimpleEraseCurrentWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.CreateKey(_key3);
                    tr.EraseCurrent();
                    Assert.True(tr.FindFirstKey());
                    Assert.Equal(_key1, tr.GetKeyAsByteArray());
                    Assert.True(tr.FindNextKey());
                    Assert.Equal(_key2, tr.GetKeyAsByteArray());
                    Assert.False(tr.FindNextKey());
                    Assert.Equal(2, tr.GetKeyValueCount());
                }
            }
        }

        [Fact]
        public void AdvancedEraseRangeWorks()
        {
            foreach(var range in EraseRangeSource())
                AdvancedEraseRangeWorksCore(range[0], range[1], range[2]);
        }

        void AdvancedEraseRangeWorksCore(int createKeys, int removeStart, int removeCount)
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                var key = new byte[2];
                using (var tr = db.StartTransaction())
                {
                    for (int i = 0; i < createKeys; i++)
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
                    for (int i = 0; i < createKeys; i++)
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
        }

        // ReSharper disable UnusedMember.Global
        public static IEnumerable<int[]> EraseRangeSource()
            // ReSharper restore UnusedMember.Global
        {
            yield return new[] { 1, 0, 1 };
            for (int i = 11; i < 1000; i += i)
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
        public void ALotOf5KBTransactionsWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                for (int i = 0; i < 5000; i++)
                {
                    var key = new byte[5000];
                    using (var tr = db.StartTransaction())
                    {
                        key[0] = (byte)(i / 256);
                        key[1] = (byte)(i % 256);
                        Assert.True(tr.CreateKey(key));
                        tr.Commit();
                    }
                }
            }
        }

        [Fact]
        public void SetKeyPrefixInOneTransaction()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
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
                        tr.SetKeyPrefix(ByteBuffer.NewSync(key, 0, 4));
                        Assert.Equal(100, tr.GetKeyValueCount());
                    }
                }
            }
        }

        [Fact]
        public void CompressibleValueLoad()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateOrUpdateKeyValue(_key1, new byte[1000]);
                    Assert.Equal(new byte[1000], tr.GetValueAsByteArray());
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void StartWritingTransactionWorks()
        {
            using (IKeyValueDB db = new InMemoryKeyValueDB())
            {
                var tr1 = db.StartWritingTransaction().Result;
                var tr2Task = db.StartWritingTransaction();
                var task = Task.Factory.StartNew(() =>
                    {
                        var tr2 = tr2Task.Result;
                        Assert.True(tr2.FindExactKey(_key1));
                        tr2.CreateKey(_key2);
                        tr2.Commit();
                        tr2.Dispose();
                    });
                tr1.CreateKey(_key1);
                tr1.Commit();
                tr1.Dispose();
                task.Wait(1000);
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.FindExactKey(_key1));
                    Assert.True(tr.FindExactKey(_key2));
                }
            }
        }

        readonly byte[] _key1 = { 1, 2, 3 };
        readonly byte[] _key2 = { 1, 3, 2 };
        readonly byte[] _key3 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    }
}