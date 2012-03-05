using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class KeyValueDBTest
    {
        [Test]
        public void CreateEmptyDatabase()
        {
            using (var stream = CreateTestStream())
            {
                using (new KeyValueDB(stream))
                {
                }
            }
        }

        [Test]
        public void FirstTransaction()
        {
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateKey(_key1));
                    tr.Commit();
                }
            }
        }

        [Test]
        public void HumanReadableDescriptionInHeaderWorks()
        {
            using (var stream = CreateTestStream())
            {
                using (IKeyValueDB db = new KeyValueDB(stream))
                {
                    (db as IKeyValueDBInOneFile).HumanReadableDescriptionInHeader = "Hello World";
                    Assert.AreEqual("Hello World", (db as IKeyValueDBInOneFile).HumanReadableDescriptionInHeader);
                }
                using (IKeyValueDB db = new KeyValueDB(stream))
                {
                    Assert.AreEqual("Hello World", (db as IKeyValueDBInOneFile).HumanReadableDescriptionInHeader);
                }
            }
        }

        [Test]
        public void MoreComplexTransaction()
        {
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
            {
                using (var tr = db.StartTransaction())
                {
                    Assert.True(tr.CreateKey(_key1));
                    Assert.False(tr.CreateKey(_key1));
                    Assert.False(tr.FindExactKey(_key2));
                    Assert.True(tr.CreateKey(_key2));
                    Assert.True(tr.FindExactKey(_key1));
                    Assert.True(tr.FindExactKey(_key2));
                    Assert.False(tr.FindExactKey(_key3));
                    tr.Commit();
                }
            }
        }

        [Test]
        public void CommitWorks()
        {
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            {
                using (IKeyValueDB db = new KeyValueDB(stream))
                {
                    using (var tr1 = db.StartTransaction())
                    {
                        tr1.CreateKey(key);
                        tr1.Commit();
                    }
                    using (var tr2 = db.StartTransaction())
                    {
                        Assert.True(tr2.FindExactKey(key));
                        var buf = tr2.GetKeyAsByteArray();
                        Assert.AreEqual(key, buf);
                    }
                }
            }
        }

        [Test]
        public void TwoTransactions()
        {
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
        public void SimpleFindNextKeyWorks()
        {
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
        public void SetValueWorksSameTransaction([Values(0, 1, 256, 268, 269, 270, 512, 4364, 4365, 4366, 5000, 1200000, 1200012, 10000000)] int firstLength,
            [Values(0, 1, 256, 268, 269, 270, 512, 4364, 4365, 4366, 5000, 1200000, 1200012, 10000000)] int secondLength)
        {
            var valbuf = new byte[secondLength];
            new Random(0).NextBytes(valbuf);
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    tr1.CreateKey(_key2);
                    tr1.CreateKey(_key3);
                    tr1.SetValue(new byte[firstLength]);
                    tr1.SetValue(valbuf);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.True(tr2.FindExactKey(_key1));
                    Assert.True(tr2.FindExactKey(_key2));
                    Assert.True(tr2.FindExactKey(_key3));
                    var valbuf2 = tr2.GetValueAsByteArray();
                    for (int i = 0; i < secondLength; i++)
                    {
                        if (valbuf[i] != valbuf2[i])
                            Assert.AreEqual(valbuf[i], valbuf2[i]);
                    }
                }
            }
        }

        [Test]
        public void SetValueWorksDifferentTransaction([Values(0, 1, 256, 268, 269, 270, 512, 4364, 4365, 4366, 5000, 1200000, 1200012, 10000000)] int firstLength,
            [Values(0, 1, 256, 268, 269, 270, 512, 4364, 4365, 4366, 5000, 1200000, 1200012, 10000000)] int secondLength)
        {
            var valbuf = new byte[secondLength];
            new Random(0).NextBytes(valbuf);
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(_key1);
                    tr1.CreateKey(_key2);
                    tr1.CreateKey(_key3);
                    tr1.SetValue(new byte[firstLength]);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.True(tr2.FindExactKey(_key1));
                    Assert.True(tr2.FindExactKey(_key2));
                    Assert.True(tr2.FindExactKey(_key3));
                    tr2.SetValue(valbuf);
                    tr2.Commit();
                }
                using (var tr3 = db.StartTransaction())
                {
                    Assert.True(tr3.FindExactKey(_key1));
                    Assert.True(tr3.FindExactKey(_key2));
                    Assert.True(tr3.FindExactKey(_key3));
                    var valbuf2 = tr3.GetValueAsByteArray();
                    for (int i = 0; i < secondLength; i++)
                    {
                        if (valbuf[i] != valbuf2[i])
                            Assert.AreEqual(valbuf[i], valbuf2[i]);
                    }
                }
            }
        }

        [Test]
        public void CreateOrUpdateKeyValueWorks([Values(0, 1, 256, 268, 269, 270, 512, 4364, 4365, 4366, 5000, 1200000, 1200012, 10000000)] int length)
        {
            var valbuf = new byte[length];
            new Random(0).NextBytes(valbuf);
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
            {
                using (var tr1 = db.StartTransaction())
                {
                    Assert.True(tr1.CreateOrUpdateKeyValue(_key1, valbuf));
                    Assert.False(tr1.CreateOrUpdateKeyValue(_key1, valbuf));
                    Assert.True(tr1.CreateOrUpdateKeyValue(_key2, valbuf));
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.SetKeyPrefix(ByteBuffer.NewAsync(_key2, 0, 1));
                    Assert.True(tr.FindFirstKey());
                    Assert.True(tr.FindNextKey());
                    Assert.AreEqual(_key2.Skip(1).ToArray(), tr.GetKeyAsByteArray());
                    Assert.False(tr.FindNextKey());
                    tr.Commit();
                }
            }
        }

        [Test]
        public void PrefixWithFindPrevKeyWorks()
        {
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(_key1);
                    tr.CreateKey(_key2);
                    tr.SetKeyPrefix(ByteBuffer.NewAsync(_key2, 0, 1));
                    Assert.True(tr.FindFirstKey());
                    Assert.AreEqual(_key1.Skip(1).ToArray(), tr.GetKeyAsByteArray());
                    Assert.False(tr.FindPreviousKey());
                    tr.Commit();
                }
            }
        }

        [Test]
        public void SimpleEraseCurrentWorks()
        {
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
                            Assert.False(tr.FindExactKey(key));
                        }
                        else
                        {
                            Assert.True(tr.FindExactKey(key));
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
            for (int i = 1001; i < 10000; i += 1000)
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
        public void RandomlyCreatedTest1()
        {
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
            {
                using (var tr = db.StartTransaction())
                {
                    tr.CreateKey(new byte[232]);
                    tr.SetValue(new byte[93341]);
                    tr.SetValue(new byte[15397]);
                    tr.SetValue(new byte[46700]);
                    tr.Commit();
                }
            }
        }

        [Test]
        public void RandomlyCreatedTest2()
        {
            using (var stream = CreateTestStream())
            using (var db = new KeyValueDB(stream))
            {
                using (var tr1 = db.StartTransaction())
                {
                    tr1.CreateKey(new byte[] { 1 });
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    tr2.CreateKey(new byte[] { 2 });
                    tr2.Commit();
                }
                using (var tr3 = db.StartTransaction())
                {
                    tr3.CreateKey(new byte[] { 3 });
                }
                using (var tr4 = db.StartTransaction())
                {
                    tr4.CreateKey(new byte[] { 4 });
                }
            }
        }

        [Test]
        public void ALotOfLargeKeysWorks()
        {
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
            {
                for (int i = 0; i < 10; i++)
                {
                    using (var tr = db.StartTransaction())
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            var key = new byte[5000];
                            key[0] = (byte)i;
                            key[key.Length - 1] = (byte)j;
                            Assert.True(tr.CreateKey(key));
                            tr.SetValue(new byte[4000 + i * 100 + j]);
                        }
                        tr.Commit();
                    }
                }
                using (var tr = db.StartTransaction())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            var key = new byte[5000];
                            key[0] = (byte)i;
                            key[key.Length - 1] = (byte)j;
                            Assert.True(tr.FindExactKey(key));
                            Assert.AreEqual(4000 + i * 100 + j, tr.GetValue().Length);
                        }
                    }
                }
            }
        }

        [Test]
        public void ALotOf5KBTransactionsWorks()
        {
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
            {
                for (int i = 0; i < 5000; i++)
                {
                    var key = new byte[5000];
                    using (var tr = db.StartTransaction())
                    {
                        key[0] = (byte)(i/256);
                        key[1] = (byte)(i%256);
                        Assert.True(tr.CreateKey(key));
                        tr.Commit();
                    }
                }
            }
        }

        [Test]
        public void SetKeyPrefixInOneTransaction()
        {
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
            using (var stream = CreateTestStream())
            using (IKeyValueDB db = new KeyValueDB(stream))
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
        public void RepairsOnReopen([Values(false, true)] bool durable)
        {
            using (var stream = CreateTestStream())
            {
                using (IKeyValueDB db = new KeyValueDB(stream))
                {
                    db.DurableTransactions = durable;
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
                    using (IKeyValueDB db2 = new KeyValueDB(stream))
                    {
                        using (var tr = db2.StartTransaction())
                        {
                            Assert.True(tr.FindExactKey(_key1));
                            Assert.True(tr.FindExactKey(_key2));
                        }
                    }
                }
                using (IKeyValueDB db = new KeyValueDB(stream))
                {
                    using (var tr = db.StartTransaction())
                    {
                        Assert.True(tr.FindExactKey(_key1));
                        Assert.True(tr.FindExactKey(_key2));
                    }
                }
            }
        }

        readonly byte[] _key1 = new byte[] { 1, 2, 3 };
        readonly byte[] _key2 = new byte[] { 1, 3, 2 };
        readonly byte[] _key3 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

        static IPositionLessStream CreateTestStream(bool log = false)
        {
            if (log)
            {
                return new LoggingPositionLessStream(new MemoryPositionLessStream(), true, LogDebug);
            }
            return new MemoryPositionLessStream();
        }

        static void LogDebug(string s)
        {
            Debug.WriteLine(s);
        }
    }
}
