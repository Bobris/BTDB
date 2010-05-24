using System.Diagnostics;
using System.IO;
using BTDB;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class BTDBTest
    {
        [Test]
        public void CreateEmptyDatabase()
        {
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
            {
                using (ILowLevelDB db = new LowLevelDB())
                {
                    Assert.IsTrue(db.Open(stream, false));
                }
            }
        }

        [Test]
        public void OpenEmptyDatabase()
        {
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
            {
                using (ILowLevelDB db = new LowLevelDB())
                {
                    Assert.IsTrue(db.Open(stream, false));
                }
                using (ILowLevelDB db = new LowLevelDB())
                {
                    Assert.IsFalse(db.Open(stream, false));
                }
            }
        }

        [Test]
        public void FirstTransaction()
        {
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr = db.StartTransaction())
                {
                    Assert.AreEqual(FindKeyResult.Created, tr.FindKey(_key1, 0, _key1.Length, FindKeyStrategy.Create));
                    tr.Commit();
                }
            }
        }

        [Test]
        public void MoreComplexTransaction()
        {
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr = db.StartTransaction())
                {
                    Assert.AreEqual(FindKeyResult.Created, tr.FindKey(_key1, 0, _key1.Length, FindKeyStrategy.Create));
                    Assert.AreEqual(FindKeyResult.FoundExact, tr.FindKey(_key1, 0, _key1.Length, FindKeyStrategy.Create));
                    Assert.AreEqual(FindKeyResult.NotFound, tr.FindKey(_key2, 0, _key2.Length, FindKeyStrategy.ExactMatch));
                    Assert.AreEqual(FindKeyResult.Created, tr.FindKey(_key2, 0, _key2.Length, FindKeyStrategy.Create));
                    Assert.AreEqual(FindKeyResult.FoundExact, tr.FindKey(_key1, 0, _key1.Length, FindKeyStrategy.ExactMatch));
                    Assert.AreEqual(FindKeyResult.FoundExact, tr.FindKey(_key2, 0, _key2.Length, FindKeyStrategy.ExactMatch));
                    Assert.AreEqual(FindKeyResult.NotFound, tr.FindKey(_key3, 0, _key3.Length, FindKeyStrategy.ExactMatch));
                    tr.Commit();
                }
            }
        }

        [Test]
        public void CommitWorks()
        {
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, s => Debug.WriteLine(s)))
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr1 = db.StartTransaction())
                {
                    tr1.FindKey(_key1, 0, _key1.Length, FindKeyStrategy.Create);
                    using (var tr2 = db.StartTransaction())
                    {
                        Assert.AreEqual(FindKeyResult.NotFound,
                                        tr2.FindKey(_key1, 0, _key1.Length, FindKeyStrategy.ExactMatch));
                    }
                    tr1.Commit();
                }
                using (var tr3 = db.StartTransaction())
                {
                    Assert.AreEqual(FindKeyResult.FoundExact,
                                    tr3.FindKey(_key1, 0, _key1.Length, FindKeyStrategy.ExactMatch));
                }
            }
        }

        [Test]
        public void RollbackWorks()
        {
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, s => Debug.WriteLine(s)))
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr1 = db.StartTransaction())
                {
                    tr1.FindKey(_key1, 0, _key1.Length, FindKeyStrategy.Create);
                    // Rollback because of missing commit
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.AreEqual(FindKeyResult.NotFound,
                                    tr2.FindKey(_key1, 0, _key1.Length, FindKeyStrategy.ExactMatch));
                }
            }
        }

        [Test]
        public void BiggerKey([Range(269, 300)] int keyLength)
        {
            var key = new byte[keyLength];
            var buf = new byte[keyLength];
            for (int i = 0; i < keyLength; i++) key[i] = (byte)i;
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, Nothing))
            using (ILowLevelDB db = new LowLevelDB())
            {
                db.Open(stream, false);
                using (var tr1 = db.StartTransaction())
                {
                    tr1.FindKey(key, 0, key.Length, FindKeyStrategy.Create);
                    tr1.Commit();
                }
                using (var tr2 = db.StartTransaction())
                {
                    Assert.AreEqual(FindKeyResult.FoundExact,
                                    tr2.FindKey(key, 0, keyLength, FindKeyStrategy.ExactMatch));
                    tr2.ReadKey(0, keyLength, buf, 0);
                    Assert.AreEqual(key, buf);
                }
            }
        }

        readonly byte[] _key1 = new byte[] { 1, 2, 3 };
        readonly byte[] _key2 = new byte[] { 1, 3, 2 };
        readonly byte[] _key3 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

        private static void Nothing(string s)
        {
        }

        private static void LogDebug(string s)
        {
            Debug.WriteLine(s);
        }
    }
}
