using System;
using System.Globalization;
using System.IO;
using System.Text;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace SimpleTester
{
    class ToStringFastVisitor : IODBFastVisitor
    {
        protected readonly StringBuilder Builder = new StringBuilder();

        public override string ToString()
        {
            return Builder.ToString();
        }

        public void MarkCurrentKeyAsUsed(IKeyValueDBTransaction tr)
        {
            Builder.Append("Used key: ");
            Print(ByteBuffer.NewSync(tr.GetKeyPrefix()));
            Builder.Append('|');
            Print(tr.GetKey());
            Builder.AppendFormat(" Value len:{0}", tr.GetStorageSizeOfCurrentKey().Value);
            Builder.AppendLine();
        }

        void Print(ByteBuffer b)
        {
            for (int i = 0; i < b.Length; i++)
            {
                if (i > 0) Builder.Append(' ');
                Builder.Append(b[i].ToString("X2"));
            }
        }
    }

    class ToStringVisitor : ToStringFastVisitor, IODBVisitor
    {
        public bool VisitSingleton(uint tableId, string tableName, ulong oid)
        {
            Builder.AppendFormat("Singleton {0}-{1} oid:{2}", tableId, tableName ?? "?Unknown?", oid);
            Builder.AppendLine();
            return true;
        }

        public bool StartObject(ulong oid, uint tableId, string tableName, uint version)
        {
            Builder.AppendFormat("Object oid:{0} {1}-{2} version:{3}", oid, tableId, tableName ?? "?Unknown?",
                version);
            Builder.AppendLine();
            return true;
        }

        public bool StartField(string name)
        {
            Builder.AppendLine($"StartField {name}");
            return true;
        }

        public bool NeedScalarAsObject()
        {
            return true;
        }

        public void ScalarAsObject(object content)
        {
            Builder.AppendLine(string.Format(CultureInfo.InvariantCulture, "ScalarObj {0}", content));
        }

        public bool NeedScalarAsText()
        {
            return true;
        }

        public void ScalarAsText(string content)
        {
            Builder.AppendLine($"ScalarStr {content}");
        }

        public void OidReference(ulong oid)
        {
            Builder.AppendLine($"OidReference {oid}");
        }

        public bool StartInlineObject(uint tableId, string tableName, uint version)
        {
            Builder.AppendLine($"StartInlineObject {tableId}-{tableName}-{version}");
            return true;
        }

        public void EndInlineObject()
        {
            Builder.AppendLine("EndInlineObject");
        }

        public bool StartList()
        {
            Builder.AppendLine("StartList");
            return true;
        }

        public bool StartItem()
        {
            Builder.AppendLine("StartItem");
            return true;
        }

        public void EndItem()
        {
            Builder.AppendLine("EndItem");
        }

        public void EndList()
        {
            Builder.AppendLine("EndList");
        }

        public bool StartDictionary()
        {
            Builder.AppendLine("StartDictionary");
            return true;
        }

        public bool StartDictKey()
        {
            Builder.AppendLine("StartDictKey");
            return true;
        }

        public void EndDictKey()
        {
            Builder.AppendLine("EndDictKey");
        }

        public bool StartDictValue()
        {
            Builder.AppendLine("StartDictValue");
            return true;
        }

        public void EndDictValue()
        {
            Builder.AppendLine("EndDictValue");
        }

        public void EndDictionary()
        {
            Builder.AppendLine("EndDictionary");
        }

        public void EndField()
        {
            Builder.AppendLine("EndField");
        }

        public void EndObject()
        {
            Builder.AppendLine("EndObject");
        }
    }

    static class Program
    {
        static void Main(string[] args)
        {
            var st = File.OpenRead(@"c:\Research\inachis\Inachis_21311");
            var kvdb = new InMemoryKeyValueDB();
            var odb = new ObjectDB();
            odb.Open(kvdb,true);
            using (var tr = kvdb.StartTransaction())
            {
                KeyValueDBExportImporter.Import(tr, st);
                tr.Commit();
            }
            using (var tr = odb.StartTransaction())
            {
                var visitor = new ToStringVisitor();
                var iterator = new ODBIterator(tr, visitor);
                iterator.Iterate();
                var text = visitor.ToString();
                Console.WriteLine(text);
            }
            //new KeyValueDBReplayer("bug.log").Replay();
            //new SpeedTest1().Test();
            //new ChannelSpeedTest().Run(args);
            //new RxSpeedTest().Run();
            //new ComplexServiceTest().Run();
            //new KeyValueSpeedTest(false,true).Run();
            //new EventStorageSpeedTestAwaitable().Run();
            //new EventStorageSpeedTestDisruptor().Run();
            //new EventStorageSpeedTest().Run();
        }
    }
}
