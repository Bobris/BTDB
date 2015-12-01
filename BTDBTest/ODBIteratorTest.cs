using System;
using System.Collections.Generic;
using System.Text;
using ApprovalTests;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest
{
    public class ODBIteratorTest : IDisposable
    {
        IKeyValueDB _lowDb;
        IObjectDB _db;

        public ODBIteratorTest()
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

        class ToStringVisitor : IODBVisitor
        {
            readonly StringBuilder _builder = new StringBuilder();

            public override string ToString()
            {
                return _builder.ToString();
            }

            public void MarkCurrentKeyAsUsed(IKeyValueDBTransaction tr)
            {
                _builder.Append("Used key: ");
                Print(ByteBuffer.NewSync(tr.GetKeyPrefix()));
                _builder.Append('|');
                Print(tr.GetKey());
                _builder.AppendFormat(" Value len:{0}", tr.GetStorageSizeOfCurrentKey().Value);
                _builder.AppendLine();
            }

            void Print(ByteBuffer b)
            {
                for (int i = 0; i < b.Length; i++)
                {
                    if (i > 0) _builder.Append(' ');
                    _builder.Append(b[i].ToString("X2"));
                }
            }

            public bool VisitSingleton(uint tableId, string tableName, ulong oid)
            {
                _builder.AppendFormat("Singleton {0}-{1} oid:{2}", tableId, tableName ?? "?Unknown?", oid);
                _builder.AppendLine();
                return true;
            }

            public bool StartObject(ulong oid, uint tableId, string tableName, uint version)
            {
                _builder.AppendFormat("Object oid:{0} {1}-{2} version:{3}", oid, tableId, tableName ?? "?Unknown?",
                    version);
                _builder.AppendLine();
                return true;
            }

            public bool StartField(string name)
            {
                throw new NotImplementedException();
            }

            public bool SimpleField(object content)
            {
                throw new NotImplementedException();
            }

            public bool EndField()
            {
                throw new NotImplementedException();
            }

            public bool VisitFieldText(string name, string content)
            {
                throw new NotImplementedException();
            }

            public void VisitOidField(string name, ulong oid)
            {
                throw new NotImplementedException();
            }

            public bool StartDictionary(string name)
            {
                throw new NotImplementedException();
            }

            public bool StartDictKey()
            {
                throw new NotImplementedException();
            }

            public void EndDictKey()
            {
                throw new NotImplementedException();
            }

            public bool StartDictValue()
            {
                throw new NotImplementedException();
            }

            public void EndDictionary()
            {
                throw new NotImplementedException();
            }

            public void EndObject()
            {
                _builder.AppendLine("EndObject");
            }
        }

        [Fact]
        public void Basics()
        {
            using (var tr = _db.StartTransaction())
            {
                var jobs = tr.Singleton<JobMap>();
                jobs.Jobs[1] = new Job { Duty = new Duty { Name = "HardCore Code" } };
                tr.Commit();
            }
            using (var tr = _db.StartReadOnlyTransaction())
            {
                var visitor = new ToStringVisitor();
                var iterator = new ODBIterator(tr, visitor);
                iterator.Iterate();
                var text = visitor.ToString();
                Approvals.Verify(text);
            }
        }

        public void Dispose()
        {
            _db.Dispose();
            _lowDb.Dispose();
        }
    }
}
