using System;
using System.Globalization;
using BTDB.ODBLayer;

namespace ODbDump.Visitor
{
    class ToConsoleVisitor : ToConsoleFastVisitor, IODBVisitor
    {
        public bool VisitSingleton(uint tableId, string? tableName, ulong oid)
        {
            Console.WriteLine("Singleton {0}-{1} oid:{2}", tableId, tableName ?? "?Unknown?", oid);
            return true;
        }

        public bool StartObject(ulong oid, uint tableId, string? tableName, uint version)
        {
            Console.WriteLine("Object oid:{0} {1}-{2} version:{3}", oid, tableId, tableName ?? "?Unknown?",
                version);
            return true;
        }

        public bool StartField(string name)
        {
            Console.WriteLine($"StartField {name}");
            return true;
        }

        public bool NeedScalarAsObject()
        {
            return true;
        }

        public void ScalarAsObject(object? content)
        {
            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "ScalarObj {0}", content));
        }

        public bool NeedScalarAsText()
        {
            return true;
        }

        public void ScalarAsText(string content)
        {
            Console.WriteLine($"ScalarStr {content}");
        }

        public void OidReference(ulong oid)
        {
            Console.WriteLine($"OidReference {oid}");
        }

        public bool StartInlineObject(uint tableId, string? tableName, uint version)
        {
            Console.WriteLine($"StartInlineObject {tableId}-{tableName}-{version}");
            return true;
        }

        public void EndInlineObject()
        {
            Console.WriteLine("EndInlineObject");
        }

        public bool StartList()
        {
            Console.WriteLine("StartList");
            return true;
        }

        public bool StartItem()
        {
            Console.WriteLine("StartItem");
            return true;
        }

        public void EndItem()
        {
            Console.WriteLine("EndItem");
        }

        public void EndList()
        {
            Console.WriteLine("EndList");
        }

        public bool StartDictionary()
        {
            Console.WriteLine("StartDictionary");
            return true;
        }

        public bool StartDictKey()
        {
            Console.WriteLine("StartDictKey");
            return true;
        }

        public void EndDictKey()
        {
            Console.WriteLine("EndDictKey");
        }

        public bool StartDictValue()
        {
            Console.WriteLine("StartDictValue");
            return true;
        }

        public void EndDictValue()
        {
            Console.WriteLine("EndDictValue");
        }

        public void EndDictionary()
        {
            Console.WriteLine("EndDictionary");
        }

        public bool StartSet()
        {
            Console.WriteLine("StartSet");
            return true;
        }

        public bool StartSetKey()
        {
            Console.WriteLine("StartSetKey");
            return true;
        }

        public void EndSetKey()
        {
            Console.WriteLine("EndSetKey");
        }

        public void EndSet()
        {
            Console.WriteLine("EndSet");
        }

        public void EndField()
        {
            Console.WriteLine("EndField");
        }

        public void EndObject()
        {
            Console.WriteLine("EndObject");
        }

        public bool StartRelation(ODBIteratorRelationInfo relationInfo)
        {
            Console.WriteLine($"Relation {relationInfo.Name}");
            return true;
        }

        public bool StartRelationKey()
        {
            Console.WriteLine("BeginKey");
            return true;
        }

        public void EndRelationKey()
        {
            Console.WriteLine("EndKey");
        }

        public bool StartRelationValue()
        {
            Console.WriteLine("BeginValue");
            return true;
        }

        public void EndRelationValue()
        {
            Console.WriteLine("EndValue");
        }

        public void InlineBackRef(int iid)
        {
            Console.WriteLine($"Inline back ref {iid}");
        }

        public void InlineRef(int iid)
        {
            Console.WriteLine($"Inline ref {iid}");
        }

        public void EndRelation()
        {
        }
    }
}
