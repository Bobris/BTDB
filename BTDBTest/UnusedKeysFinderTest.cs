using System;
using System.Collections.Generic;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest
{
    public class UnusedKeysFinderTest : IDisposable
    {
        IKeyValueDB _lowDb;
        IObjectDB _db;

        public UnusedKeysFinderTest()
        {
            _lowDb = new InMemoryKeyValueDB();
            OpenDb();
        }

        void OpenDb()
        {
            _db = new ObjectDB();
            _db.Open(_lowDb, false);
        }

        public class Duty
        {            
            public string Name { get; set; }
        }

        public class Job
        {
            public Duty Duty { get; set; }
        }

        public class JobMap
        {
            public IDictionary<ulong, Job> Jobs { get; set; }
        }

        class SimpleKeyStore : IUsedKeysStore
        {
            string _currentCtx = String.Empty;
            readonly Stack<int> _ctxLengths = new Stack<int>();
            readonly IDictionary<byte[], string> _keys = new Dictionary<byte[], string>(); //key, ctx

            public void PushContext(string name)
            {
                _ctxLengths.Push(_currentCtx.Length);
                _currentCtx += "." + name;               
            }

            public void Add(byte[] key)
            {
                _keys.Add(key, _currentCtx);
            }

            public void Add(byte[] prefix, byte[] suffix)
            {
                var key = new byte[prefix.Length + suffix.Length];
                Buffer.BlockCopy(prefix, 0, key, 0, prefix.Length);
                Buffer.BlockCopy(suffix, 0, key, prefix.Length, suffix.Length);
                Add(key);
            }

            public IDictionary<byte[], string> Keys => _keys;

            public void PopContext()
            {
                _currentCtx = _currentCtx.Substring(0, _ctxLengths.Pop());
            }
        }

        [Fact]
        public void CanEnumerateUsedObject()
        {
            using (var tr = _db.StartTransaction())
            {
                var jobs = tr.Singleton<JobMap>();
                jobs.Jobs[1] = new Job { Duty = new Duty { Name = "Code"} };
                tr.Commit();
            }
            using (var tr = _db.StartReadOnlyTransaction())
            {
                var store = new SimpleKeyStore();
                var traversal = new UsedKeysIterator();
                traversal.Iterate(tr, store);
                Assert.True(store.Keys.Count > 0);
            }
                
        }

        public void Dispose()
        {
            _db.Dispose();
            _lowDb.Dispose();
        }

    }
}
