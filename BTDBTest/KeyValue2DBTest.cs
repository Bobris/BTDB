using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KV2DBLayer;
using BTDB.KVDBLayer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class KeyValue2DBTest
    {
        [Test]
        public void CreateEmptyDatabase()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (new KeyValue2DB(fileCollection))
            {
            }
        }

        [Test]
        public void FirstTransaction()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    tr.Commit();
                }
            }
        }

        [Test]
        public void MoreComplexTransaction()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    Assert.False(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key1), ByteBuffer.NewAsync(new byte[0])));
                    Assert.AreEqual(FindResult.Previous, tr.Find(ByteBuffer.NewAsync(_key2)));
                    Assert.True(tr.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(_key2), ByteBuffer.NewAsync(new byte[0])));
                    Assert.AreEqual(FindResult.Exact, tr.Find(ByteBuffer.NewAsync(_key1)));
                    Assert.AreEqual(FindResult.Exact, tr.Find(ByteBuffer.NewAsync(_key2)));
                    Assert.AreEqual(FindResult.Previous, tr.Find(ByteBuffer.NewAsync(_key3)));
                    Assert.AreEqual(FindResult.Next, tr.Find(ByteBuffer.NewEmpty()));
                    tr.Commit();
                }
            }
        }

        [Test]
        public void CommitWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    using (var tr2 = db.StartTransaction())
                    {
                        Assert.False(tr2.FindExactKey(_key1));
                    }
                    tr1.Commit();
                }
                using (var tr3 = db.StartTransaction())
                {
                    Assert.True(tr3.FindExactKey(_key1));
                }
            }
        }

        [Test]
        public void RollbackWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    // Rollback because of missing commit
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.False(tr2.FindExactKey(_key1));
                }
            }
        }

        [Test]
        public void OnlyOneWrittingTransactionPossible()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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

        [Test]
        public void OnlyOneWrittingTransactionPossible2()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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

        [Test]
        public void BiggerKey([Values(0, 1, 268, 269, 270, 4364, 4365, 4366, 1200000)] int keyLength)
        {
            var key = new byte[keyLength];
            for (int i = 0; i < keyLength; i++) key[i] = (byte)i;
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(key);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.True(tr2.FindExactKey(key));
                    Assert.AreEqual(key, tr2.GetKeyAsByteArray());
                }
            }
        }

        [Test]
        public void TwoTransactions()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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

        [Test]
        public void MultipleTransactions([Values(1000)] int transactionCount)
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                var key = new byte[2];
                for (int i = 0; i < transactionCount; i++)
                {
                    key[0] = (byte)(i / 256);
                    key[1] = (byte)(i % 256);
                    using (var tr1 = db.StartTransaction())
                    {
                        tr1.CreateKey(key);
                        if (i % 100 == 0 || i == transactionCount - 1)
                        {
                            for (int j = 0; j < i; j++)
                            {
                                key[0] = (byte)(j / 256);
                                key[1] = (byte)(j % 256);
                                Assert.True(tr1.FindExactKey(key));
                            }
                        }
                        tr1.Commit();
                    }
                }
            }
        }

        [Test]
        public void MultipleTransactions2([Values(1000)] int transactionCount)
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                var key = new byte[2];
                for (int i = 0; i < transactionCount; i++)
                {
                    key[0] = (byte)((transactionCount - i) / 256);
                    key[1] = (byte)((transactionCount - i) % 256);
                    using (var tr1 = db.StartTransaction())
                    {
                        tr1.CreateKey(key);
                        if (i % 100 == 0 || i == transactionCount - 1)
                        {
                            for (int j = 0; j < i; j++)
                            {
                                key[0] = (byte)((transactionCount - j) / 256);
                                key[1] = (byte)((transactionCount - j) % 256);
                                Assert.True(tr1.FindExactKey(key));
                            }
                        }
                        tr1.Commit();
                    }
                }
            }
        }

        [Test]
        public void SimpleFindPreviousKeyWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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
                    Assert.AreEqual(_key1, tr2.GetKeyAsByteArray());
                    Assert.False(tr2.FindPreviousKey());
                }
            }
        }

        [Test]
        public void FindKeyWithPreferPreviousKeyWorks()
        {
            const int keyCount = 10000;
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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
                        Assert.AreEqual(FindResult.Previous, findKeyResult);
                        Assert.AreEqual(i, tr.GetKeyIndex());
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
                            Assert.AreEqual(FindResult.Next, findKeyResult);
                            Assert.AreEqual(i, tr.GetKeyIndex());
                        }
                        else
                        {
                            Assert.AreEqual(FindResult.Previous, findKeyResult);
                            Assert.AreEqual(i - 1, tr.GetKeyIndex());
                        }
                    }
                }
            }
        }

        [Test]
        public void SimpleFindNextKeyWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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
                    Assert.AreEqual(_key2, tr2.GetKeyAsByteArray());
                    Assert.False(tr2.FindNextKey());
                }
            }
        }

        [Test]
        public void AdvancedFindPreviousAndNextKeyWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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
                    Assert.AreEqual(-1, tr.GetKeyIndex());
                    tr.FindExactKey(key);
                    Assert.AreEqual(keysCreated - 1, tr.GetKeyIndex());
                    for (int i = 1; i < keysCreated; i++)
                    {
                        Assert.True(tr.FindPreviousKey());
                        Assert.AreEqual(keysCreated - 1 - i, tr.GetKeyIndex());
                    }
                    Assert.False(tr.FindPreviousKey());
                    Assert.AreEqual(-1, tr.GetKeyIndex());
                    for (int i = 0; i < keysCreated; i++)
                    {
                        Assert.True(tr.FindNextKey());
                        Assert.AreEqual(i, tr.GetKeyIndex());
                    }
                    Assert.False(tr.FindNextKey());
                    Assert.AreEqual(-1, tr.GetKeyIndex());
                }
            }
        }

        [Test]
        public void SetKeyIndexWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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
                        Assert.AreEqual((byte)(i / 256), key[0]);
                        Assert.AreEqual((byte)(i % 256), key[1]);
                        Assert.AreEqual(i, tr.GetKeyIndex());
                    }
                }
            }
        }

        [Test]
        public void CreateOrUpdateKeyValueWorks([Values(0, 1, 256, 268, 269, 270, 512, 4364, 4365, 4366, 5000, 1200000, 1200012, 10000000)] int length)
        {
            var valbuf = new byte[length];
            new Random(0).NextBytes(valbuf);
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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
                            Assert.AreEqual(valbuf[i], valbuf2[i]);
                    }
                    Assert.True(tr2.FindExactKey(_key2));
                    valbuf2 = tr2.GetValueAsByteArray();
                    for (int i = 0; i < length; i++)
                    {
                        if (valbuf[i] != valbuf2[i])
                            Assert.AreEqual(valbuf[i], valbuf2[i]);
                    }
                }
            }
        }

        [Test]
        public void FindFirstKeyWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.False(tr.FindFirstKey());
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.CreateKey(_key3);
                    Assert.True(tr.FindFirstKey());
                    Assert.AreEqual(_key1, tr.GetKeyAsByteArray());
                    tr.Commit();
                }
            }
        }

        [Test]
        public void FindLastKeyWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.False(tr.FindLastKey());
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.CreateKey(_key3);
                    Assert.True(tr.FindLastKey());
                    Assert.AreEqual(_key2, tr.GetKeyAsByteArray());
                    tr.Commit();
                }
            }
        }

        [Test]
        public void SimplePrefixWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.CreateKey(_key3);
                    Assert.AreEqual(3, tr.GetKeyValueCount());
                    tr.SetKeyPrefix(ByteBuffer.NewAsync(_key1, 0, 3));
                    Assert.AreEqual(2, tr.GetKeyValueCount());
                    tr.FindFirstKey();
                    Assert.AreEqual(new byte[0], tr.GetKeyAsByteArray());
                    tr.FindLastKey();
                    Assert.AreEqual(_key3.Skip(3).ToArray(), tr.GetKeyAsByteArray());
                    tr.Commit();
                }
            }
        }

        [Test]
        public void PrefixWithFindNextKeyWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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

        [Test]
        public void PrefixWithFindPrevKeyWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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

        [Test]
        public void SimpleEraseCurrentWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.CreateKey(_key3);
                    tr.EraseCurrent();
                    Assert.True(tr.FindFirstKey());
                    Assert.AreEqual(_key1, tr.GetKeyAsByteArray());
                    Assert.True(tr.FindNextKey());
                    Assert.AreEqual(_key2, tr.GetKeyAsByteArray());
                    Assert.False(tr.FindNextKey());
                    Assert.AreEqual(2, tr.GetKeyValueCount());
                }
            }
        }

        [Test, TestCaseSource("EraseRangeSource")]
        public void AdvancedEraseRangeWorks(int createKeys, int removeStart, int removeCount)
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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
                    Assert.AreEqual(createKeys - removeCount, tr.GetKeyValueCount());
                    tr.Commit();
                }
                using (var tr = db.StartTransaction())
                {
                    Assert.AreEqual(createKeys - removeCount, tr.GetKeyValueCount());
                    for (int i = 0; i < createKeys; i++)
                    {
                        key[0] = (byte)(i / 256);
                        key[1] = (byte)(i % 256);
                        if (i >= removeStart && i < removeStart + removeCount)
                        {
                            Assert.False(tr.FindExactKey(key), "{0} should be removed", i);
                        }
                        else
                        {
                            Assert.True(tr.FindExactKey(key), "{0} should be found", i);
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

        [Test]
        public void ALotOf5KBTransactionsWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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

        [Test]
        public void SetKeyPrefixInOneTransaction()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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
                        Assert.AreEqual(100, tr.GetKeyValueCount());
                    }
                }
            }
        }

        [Test]
        public void StartWritingTransactionWorks()
        {
            using (var fileCollection = new InMemoryFileCollection())
            using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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

        [Test]
        public void RepairsOnReopen()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateKey(_key1);
                        tr.Commit();
                    }
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateKey(_key2);
                        tr.Commit();
                    }
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateKey(_key3);
                        // rollback
                    }
                    using (IKeyValueDB db2 = new KeyValue2DB(fileCollection))
                    {
                        using (var tr = db2.StartTransaction())
                        {
                            Assert.True(tr.FindExactKey(_key1));
                            Assert.True(tr.FindExactKey(_key2));
                            Assert.False(tr.FindExactKey(_key3));
                        }
                    }
                }
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        Assert.True(tr.FindExactKey(_key1));
                        Assert.True(tr.FindExactKey(_key2));
                        Assert.False(tr.FindExactKey(_key3));
                    }
                }
            }
        }

        [Test]
        public void MoreComplexReopen()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
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
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        var key = new byte[100];
                        key[1] = 1;
                        Assert.True(tr.FindExactKey(key));
                        tr.FindNextKey();
                        Assert.AreEqual(5, tr.GetKeyAsByteArray()[1]);
                        Assert.AreEqual(96, tr.GetKeyValueCount());
                    }
                }
            }
        }
        [Test]
        public void AddingContinueToSameFileAfterReopen()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(_key1, _key1);
                        tr.Commit();
                    }
                }
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(_key2, _key2);
                        tr.Commit();
                    }
                }
                Assert.AreEqual(1, fileCollection.GetCount());
            }
        }

        [Test]
        public void AddingContinueToNewFileAfterReopenWithCorruption()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(_key1, _key1);
                        tr.Commit();
                    }
                }
                fileCollection.GetFile(fileCollection.Enumerate().First()).SetSize(20);
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        Assert.AreEqual(0, tr.GetKeyValueCount());
                        tr.CreateOrUpdateKeyValue(_key2, _key2);
                        tr.Commit();
                    }
                }
                Assert.AreEqual(2, fileCollection.GetCount());
            }
        }

        [Test]
        public void CompressibleValueLoad()
        {
            using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValue2DB(fileCollection))
                {
                    using (var tr = db.StartTransaction())
                    {
                        tr.CreateOrUpdateKeyValue(_key1, new byte[1000]);
                        Assert.AreEqual(new byte[1000],tr.GetValueAsByteArray());
                        tr.Commit();
                    }
                }
            }
        }

        readonly byte[] _key1 = new byte[] { 1, 2, 3 };
        readonly byte[] _key2 = new byte[] { 1, 3, 2 };
        readonly byte[] _key3 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    }
}