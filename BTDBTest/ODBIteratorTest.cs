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
            IterateWithApprove();
        }

        public enum TestEnum
        {
            Item1,
            Item2
        }

        public class VariousFieldTypes
        {
            public string StringField { get; set; }
            public sbyte SByteField { get; set; }
            public byte ByteField { get; set; }
            public short ShortField { get; set; }
            public ushort UShortField { get; set; }
            public int IntField { get; set; }
            public uint UIntField { get; set; }
            public long LongField { get; set; }
            public ulong ULongField { get; set; }
            public object DbObjectField { get; set; }
            public VariousFieldTypes VariousFieldTypesField { get; set; }
            public bool BoolField { get; set; }
            public double DoubleField { get; set; }
            public float FloatField { get; set; }
            public decimal DecimalField { get; set; }
            public Guid GuidField { get; set; }
            public DateTime DateTimeField { get; set; }
            public TimeSpan TimeSpanField { get; set; }
            public TestEnum EnumField { get; set; }
            public byte[] ByteArrayField { get; set; }
            public ByteBuffer ByteBufferField { get; set; }
        }

        [Fact]
        public void FieldsOfVariousTypes()
        {
            using (var tr = _db.StartTransaction())
            {
                var o = tr.Singleton<VariousFieldTypes>();
                o.StringField = "Text";
                o.SByteField = -10;
                o.ByteField = 10;
                o.ShortField = -1000;
                o.UShortField = 1000;
                o.IntField = -100000;
                o.UIntField = 100000;
                o.LongField = -1000000000000;
                o.ULongField = 1000000000000;
                o.DbObjectField = o;
                o.VariousFieldTypesField = o;
                o.BoolField = true;
                o.DoubleField = 12.34;
                o.FloatField = -12.34f;
                o.DecimalField = 123456.789m;
                o.DateTimeField = new DateTime(2000, 1, 1, 12, 34, 56, DateTimeKind.Local);
                o.TimeSpanField = new TimeSpan(1, 2, 3, 4);
                o.GuidField = new Guid("39aabab2-9971-4113-9998-a30fc7d5606a");
                o.EnumField = TestEnum.Item2;
                o.ByteArrayField = new byte[] { 0, 1, 2 };
                o.ByteBufferField = ByteBuffer.NewAsync(new byte[] { 0, 1, 2 }, 1, 1);
                tr.Commit();
            }
            IterateWithApprove();
        }

        void IterateWithApprove()
        {
            using (var tr = _db.StartTransaction())
            {
                var visitor = new ToStringVisitor();
                var iterator = new ODBIterator(tr, visitor);
                iterator.Iterate();
                var text = visitor.ToString();
                Approvals.Verify(text);
            }
        }

        public class VariousLists
        {
            public IList<int> IntList { get; set; }
            public IList<string> StringList { get; set; }
            public IList<byte> ByteList { get; set; }
        }

        [Fact]
        public void ListOfSimpleValues()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<VariousLists>();
                root.IntList = new List<int> { 5, 10, 2000 };
                root.StringList = new List<string> { "A", null, "AB!" };
                root.ByteList = new List<byte> { 0, 255 };
                tr.Commit();
            }
            IterateWithApprove();
        }

        public void Dispose()
        {
            _db.Dispose();
            _lowDb.Dispose();
        }
    }
}
