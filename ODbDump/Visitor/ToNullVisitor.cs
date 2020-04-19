using BTDB.ODBLayer;

namespace ODbDump.Visitor
{
    class ToNullVisitor : ToConsoleFastVisitor, IODBVisitor
    {
        public bool VisitSingleton(uint tableId, string tableName, ulong oid)
        {
            return true;
        }

        public bool StartObject(ulong oid, uint tableId, string tableName, uint version)
        {
            return true;
        }

        public bool StartField(string name)
        {
            return true;
        }

        public bool NeedScalarAsObject()
        {
            return true;
        }

        public void ScalarAsObject(object content)
        {
        }

        public bool NeedScalarAsText()
        {
            return true;
        }

        public void ScalarAsText(string content)
        {
        }

        public void OidReference(ulong oid)
        {
        }

        public bool StartInlineObject(uint tableId, string tableName, uint version)
        {
            return true;
        }

        public void EndInlineObject()
        {
        }

        public bool StartList()
        {
            return true;
        }

        public bool StartItem()
        {
            return true;
        }

        public void EndItem()
        {
        }

        public void EndList()
        {
        }

        public bool StartDictionary()
        {
            return true;
        }

        public bool StartDictKey()
        {
            return true;
        }

        public void EndDictKey()
        {
        }

        public bool StartDictValue()
        {
            return true;
        }

        public void EndDictValue()
        {
        }

        public void EndDictionary()
        {
        }

        public bool StartSet()
        {
            return true;
        }

        public bool StartSetKey()
        {
            return true;
        }

        public void EndSetKey()
        {
        }

        public void EndSet()
        {
        }

        public void EndField()
        {
        }

        public void EndObject()
        {
        }

        public bool StartRelation(string relationName)
        {
            return true;
        }

        public bool StartRelationKey()
        {
            return true;
        }

        public void EndRelationKey()
        {
        }

        public bool StartRelationValue()
        {
            return true;
        }

        public void EndRelationValue()
        {
        }

        public void EndRelation()
        {
        }

        public void InlineBackRef(int iid)
        {
        }

        public void InlineRef(int iid)
        {
        }
    }
}
