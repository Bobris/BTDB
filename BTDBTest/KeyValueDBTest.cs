using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest
{
    public class KeyValueDBTest
    {
        [Fact]
        public void CreateEmptyDatabase()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (new KeyValueDB(fileCollection))
            {
            }
        }

        [Fact]
        public void EmptyTransaction()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void FirstTransactionIsNumber1()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
        public void CanGetSizeOfPair()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[1]));
                    var s = tr.GetStorageSizeOfCurrentKey();
                    Assert.Equal((uint)_key1.Length, s.Key);
                    Assert.Equal(1u, s.Value);
                }
            }
        }

        [Fact]
        public void ReadOnlyTransactionThrowsOnWriteAccess()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    Assert.False(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    Assert.Equal(FindResult.Previous, tr.Find(ByteBuffer.NewAsync(Key2)));
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(Key2), ByteBuffer.NewAsync(new byte[0])));
                    Assert.Equal(FindResult.Exact, tr.Find(ByteBuffer.NewAsync(_key1)));
                    Assert.Equal(FindResult.Exact, tr.Find(ByteBuffer.NewAsync(Key2)));
                    Assert.Equal(FindResult.Previous, tr.Find(ByteBuffer.NewAsync(_key3)));
                    Assert.Equal(FindResult.Next, tr.Find(ByteBuffer.NewEmpty()));
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void CommitWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    using (var tr2 = db.StartTransaction())
                    {
                        Assert.False(tr2.FindExactKey(_key1));
                        Assert.Throws<BTDBTransactionRetryException>(() => tr2.CreateKey(Key2));
                    }
                }
            }
        }

        [Fact]
        public void OnlyOneWrittingTransactionPossible2()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                var tr1 = db.StartTransaction();
                tr1.CreateKey(_key1);
                using (var tr2 = db.StartTransaction())
                {
                    tr1.Commit();
                    tr1.Dispose();
                    Assert.False(tr2.FindExactKey(_key1));
                    Assert.Throws<BTDBTransactionRetryException>(() => tr2.CreateKey(Key2));
                }
            }
        }

        [Fact]
        public void TwoEmptyWriteTransactionsWithNestedWaiting()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
        [InlineData(0, 0, 0)]
        [InlineData(0, 0, 1)]
        [InlineData(0, 0, 2)]
        [InlineData(0, 0, 500)]
        [InlineData(0, 0, 1200000)]
        [InlineData(0, 1, 1)]
        [InlineData(0, 1, 2)]
        [InlineData(0, 1, 500)]
        [InlineData(1, 0, 1200000)]
        [InlineData(1, 0, 1)]
        [InlineData(1, 0, 2)]
        [InlineData(1, 0, 500)]
        [InlineData(1, 0, 1200000)]
        [InlineData(1, 1, 1)]
        [InlineData(1, 1, 2)]
        [InlineData(1, 1, 500)]
        [InlineData(2000, 5000, 1200000)]
        [InlineData(2000, 5000, 1)]
        [InlineData(2000, 5000, 2)]
        [InlineData(2000, 5000, 500)]
        [InlineData(2000, 5000, 1200000)]
        public void BiggerKey(int prefixLength, int offsetKey, int keyLength)
        {
            var prefix = new byte[prefixLength];
            var keyb = new byte[offsetKey + keyLength];
            for (int i = offsetKey; i < offsetKey + keyLength; i++) keyb[i] = (byte)i;
            var key = ByteBuffer.NewAsync(keyb, offsetKey, keyLength);
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.SetKeyPrefix(prefix);
                    tr1.CreateOrUpdateKeyValue(key, ByteBuffer.NewEmpty());
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    tr2.SetKeyPrefix(prefix);
                    Assert.True(tr2.FindExactKey(key.ToByteArray()));
                    Assert.Equal(key.ToByteArray(), tr2.GetKeyAsByteArray());
                }
            }
        }

        [Fact]
        public void TwoTransactions()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
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
        }

        [Theory]
        [InlineData(1000)]
        public void MultipleTransactions(int transactionCount)
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
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
                    Assert.True(tr2.FindNextKey());
                    Assert.Equal(Key2, tr2.GetKeyAsByteArray());
                    Assert.False(tr2.FindNextKey());
                }
            }
        }

        [Fact]
        public void AdvancedFindPreviousAndNextKeyWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr1 = db.StartTransaction())
                {
                    Assert.True(tr1.CreateOrUpdateKeyValueUnsafe(_key1, valbuf));
                    Assert.False(tr1.CreateOrUpdateKeyValueUnsafe(_key1, valbuf));
                    Assert.True(tr1.CreateOrUpdateKeyValueUnsafe(Key2, valbuf));
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
                    Assert.True(tr2.FindExactKey(Key2));
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.False(tr.FindFirstKey());
                    tr.CreateKey(_key1);
                    tr.CreateKey(Key2);
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.False(tr.FindLastKey());
                    tr.CreateKey(_key1);
                    tr.CreateKey(Key2);
                    tr.CreateKey(_key3);
                    Assert.True(tr.FindLastKey());
                    Assert.Equal(Key2, tr.GetKeyAsByteArray());
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void SimplePrefixWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(Key2);
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(Key2);
                    tr.SetKeyPrefix(ByteBuffer.NewAsync(Key2, 0, 1));
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(Key2);
                    tr.SetKeyPrefix(ByteBuffer.NewAsync(Key2, 0, 1));
                    Assert.True(tr.FindFirstKey());
                    Assert.False(tr.FindPreviousKey());
                    tr.Commit();
                }
            }
        }

        [Fact]
        public void SimpleEraseCurrentWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(Key2);
                    tr.CreateKey(_key3);
                    tr.EraseCurrent();
                    Assert.True(tr.FindFirstKey());
                    Assert.Equal(_key1, tr.GetKeyAsByteArray());
                    Assert.True(tr.FindNextKey());
                    Assert.Equal(Key2, tr.GetKeyAsByteArray());
                    Assert.False(tr.FindNextKey());
                    Assert.Equal(2, tr.GetKeyValueCount());
                }
            }
        }

        [Fact]
        public void AdvancedEraseRangeWorks()
        {
            foreach (var range in EraseRangeSource())
                AdvancedEraseRangeWorks(range[0], range[1], range[2]);
        }

        void AdvancedEraseRangeWorks(int createKeys, int removeStart, int removeCount)
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValueDB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(_key1, new byte[1000]);
                        Assert.Equal(new byte[1000], tr.GetValueAsByteArray());
                        tr.Commit();
                    }
                }
            }
        }

        [Fact]
        public void StartWritingTransactionWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValueDB(fileCollection))
            {
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
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.FindExactKey(_key1));
                    Assert.True(tr.FindExactKey(Key2));
                }
            }
        }

        [Fact]
        public void RepairsOnReopen()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValueDB(fileCollection))
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
                    using (IKeyValueDB db2 = new KeyValueDB(fileCollection))
                    {
                        using (var tr = db2.StartTransaction())
                        {
                            Assert.True(tr.FindExactKey(_key1));
                            Assert.True(tr.FindExactKey(Key2));
                            Assert.False(tr.FindExactKey(_key3));
                        }
                    }
                }
                using (IKeyValueDB db = new KeyValueDB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        Assert.True(tr.FindExactKey(_key1));
                        Assert.True(tr.FindExactKey(Key2));
                        Assert.False(tr.FindExactKey(_key3));
                    }
                }
            }
        }

        [Fact]
        public void MoreComplexReopen()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValueDB(fileCollection))
                {
                    for (int i = 0; i < 100; i++)
                    {
                        var key = new byte[100];
                        using (var tr = db.StartTransaction())
                        {
                            key[0] = (byte)(i / 256);
                            key[1] = (byte)(i % 256);
                            Assert.True(tr.CreateOrUpdateKeyValue(key, key));
                            tr.Commit();
                        }
                    }
                    using (var tr = db.StartTransaction())
                    {
                        tr.SetKeyIndex(0);
                        tr.EraseCurrent();
                        tr.EraseRange(1, 3);
                        tr.Commit();
                    }
                }
                using (IKeyValueDB db = new KeyValueDB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        var key = new byte[100];
                        key[1] = 1;
                        Assert.True(tr.FindExactKey(key));
                        tr.FindNextKey();
                        Assert.Equal(5, tr.GetKeyAsByteArray()[1]);
                        Assert.Equal(96, tr.GetKeyValueCount());
                    }
                }
            }
        }

        [Fact]
        public void AddingContinueToSameFileAfterReopen()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValueDB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(_key1, _key1);
                        tr.Commit();
                    }
                }
                using (IKeyValueDB db = new KeyValueDB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(Key2, Key2);
                        tr.Commit();
                    }
                    Console.WriteLine(db.CalcStats());
                }
                Assert.Equal(2u, fileCollection.GetCount()); // Log + Index
            }
        }

        [Fact]
        public void AddingContinueToNewFileAfterReopenWithCorruption()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValueDB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(_key1, _key1);
                        tr.Commit();
                    }
                }
                fileCollection.SimulateCorruptionBySetSize(20 + 16);
                using (IKeyValueDB db = new KeyValueDB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        Assert.Equal(0, tr.GetKeyValueCount());
                        tr.CreateOrUpdateKeyValue(Key2, Key2);
                        tr.Commit();
                    }
                    Console.WriteLine(db.CalcStats());
                }
                Assert.True(2 <= fileCollection.GetCount());
            }
        }

        [Fact]
        public void AddingContinueToSameFileAfterReopenOfDBWith2TransactionLogFiles()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(_key1, new byte[1024]);
                        tr.CreateOrUpdateKeyValue(Key2, new byte[10]);
                        tr.Commit();
                    }
                }
                Assert.Equal(2u, fileCollection.GetCount());
                using (IKeyValueDB db = new KeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(Key2, new byte[1024]);
                        tr.CreateOrUpdateKeyValue(_key3, new byte[10]);
                        tr.Commit();
                    }
                }
                Assert.Equal(4u, fileCollection.GetCount());
                using (IKeyValueDB db = new KeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(Key2, Key2);
                        tr.Commit();
                    }
                }
                Assert.Equal(4u, fileCollection.GetCount());
            }
        }

        [Fact]
        public void CompactionWaitsForFinishingOldTransactionsBeforeRemovingFiles()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (var db = new KeyValueDB(fileCollection, new NoCompressionStrategy(), 1024, null))
                {
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
                    Task.Run(() =>
                    {
                        db.Compact(new CancellationToken());
                    });
                    Thread.Sleep(2000);
                    Console.WriteLine(db.CalcStats());
                    Assert.True(4 <= fileCollection.GetCount()); // 2 Logs, 1 Value, 1 KeyIndex, (optinal 1 Unknown (old KeyIndex))
                    longTr.Dispose();
                    Thread.Sleep(1000);
                    Assert.Equal(2u, fileCollection.GetCount()); // 1 Log, 1 KeyIndex
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(_key3, new byte[10]);
                        tr.Commit();
                    }
                    using (var db2 = new KeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
                    {
                        using (var tr = db2.StartTransaction())
                        {
                            Assert.True(tr.FindExactKey(_key3));
                        }
                    }
                }
            }
        }

        [Fact]
        public void PreapprovedCommitAndCompaction()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (var db = new KeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
                {
                    using (var tr = db.StartWritingTransaction().Result)
                    {
                        tr.CreateOrUpdateKeyValue(_key1, new byte[1024]);
                        tr.CreateOrUpdateKeyValue(Key2, new byte[10]);
                        tr.Commit();
                    }
                    db.Compact();
                    Thread.Sleep(2000);
                    using (var tr = db.StartWritingTransaction().Result)
                    {
                        tr.EraseRange(0, 0);
                        tr.Commit();
                    }
                    db.Compact();
                    Thread.Sleep(2000);
                    using (var db2 = new KeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
                    {
                        using (var tr = db2.StartTransaction())
                        {
                            Assert.False(tr.FindExactKey(_key1));
                            Assert.True(tr.FindExactKey(Key2));
                        }
                    }
                }
            }
        }

        [Fact]
        public void FastCleanUpOnStartRemovesUselessFiles()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (var db = new KeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(_key1, new byte[1024]);
                        tr.CreateOrUpdateKeyValue(Key2, new byte[1024]);
                        tr.Commit();
                    }
                    using (var tr = db.StartTransaction())
                    {
                        tr.EraseAll();
                        tr.Commit();
                    }
                    Assert.Equal(3u, fileCollection.GetCount()); // 3 Logs
                }
                using (var db = new KeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
                {
                    Console.WriteLine(db.CalcStats());
                    Assert.Equal(2u, fileCollection.GetCount()); // 1 Log, 1 KeyIndex
                }
            }
        }

        readonly byte[] _key1 = { 1, 2, 3 };
        readonly byte[] _key2 = { 1, 3, 2 };
        readonly byte[] _key3 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

        public byte[] Key2 => _key2;
    }
}