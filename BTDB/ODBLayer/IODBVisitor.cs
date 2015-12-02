using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public interface IODBVisitor
    {
        void MarkCurrentKeyAsUsed(IKeyValueDBTransaction tr);
        bool VisitSingleton(uint tableId, string tableName, ulong oid);
        bool StartObject(ulong oid, uint tableId, string tableName, uint version);
        bool StartField(string name);
        bool NeedScalarAsObject(); // return true if needed as object
        void ScalarAsObject(object content);
        bool NeedScalarAsText(); // return true if needed as string
        void ScalarAsText(string content);
        void OidReference(ulong oid);
        bool StartInlineObject(uint tableId, string tableName, uint version); // false to skip
        void EndInlineObject();
        bool StartList();
        bool StartItem();
        void EndItem();
        void EndList();
        bool StartDictionary(); // false to skip iteration of this Dict 
        bool StartDictKey(); // false to skip iteration of key
        void EndDictKey();
        bool StartDictValue(); // false to skip iteration of value
        void EndDictValue();
        void EndDictionary();
        void EndField();
        void EndObject();
    }
}