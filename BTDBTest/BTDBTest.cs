using System.Diagnostics;
using System.IO;
using BTDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BTDBTest
{
    [TestClass]
    public class BTDBTest
    {
        [TestMethod]
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
        [TestMethod]
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
        [TestMethod]
        public void FirstTransaction()
        {
            using (var stream = new LoggingStream(new StreamProxy(new MemoryStream(), true), true, s => Debug.WriteLine(s)))
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

        private readonly byte[] _key1 = new byte[] { 1, 2, 3 };
        
        private static void Nothing(string s)
        {
        }
    }
}
