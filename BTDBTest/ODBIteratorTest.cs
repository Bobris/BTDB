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
                _builder.AppendLine($"StartField {name}");
                return true;
            }

            public bool NeedScalarAsObject()
            {
                return true;
            }

            public void ScalarAsObject(object content)
            {
                _builder.AppendLine($"ScalarObj {content}");
            }

            public bool NeedScalarAsText()
            {
                return true;
            }

            public void ScalarAsText(string content)
            {
                _builder.AppendLine($"ScalarStr {content}");
            }

            public void OidReference(ulong oid)
            {
                _builder.AppendLine($"OidReference {oid}");
            }

            public bool StartInlineObject(uint tableId, string tableName, uint version)
            {
                _builder.AppendLine($"StartInlineObject {tableId}-{tableName}-{version}");
                return true;
            }

            public void EndInlineObject()
            {
                _builder.AppendLine("EndInlineObject");
            }

            public bool StartList()
            {
                _builder.AppendLine("StartList");
                return true;
            }

            public bool StartItem()
            {
                _builder.AppendLine("StartItem");
                return true;
            }

            public void EndItem()
            {
                _builder.AppendLine("EndItem");
            }

            public void EndList()
            {
                _builder.AppendLine("EndList");
            }

            public bool StartDictionary()
            {
                _builder.AppendLine("StartDictionary");
                return true;
            }

            public bool StartDictKey()
            {
                _builder.AppendLine("StartDictKey");
                return true;
            }

            public void EndDictKey()
            {
                _builder.AppendLine("EndDictKey");
            }

            public bool StartDictValue()
            {
                _builder.AppendLine("StartDictValue");
                return true;
            }

            public void EndDictValue()
            {
                _builder.AppendLine("EndDictValue");
            }

            public void EndDictionary()
            {
                _builder.AppendLine("EndDictionary");
            }

            public void EndField()
            {
                _builder.AppendLine("EndField");
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
