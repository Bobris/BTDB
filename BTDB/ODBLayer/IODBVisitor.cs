using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public interface IODBVisitor
    {
        void MarkCurrentKeyAsUsed(IKeyValueDBTransaction tr);
        bool VisitSingleton(uint tableId, string tableName, ulong oid);
        bool StartObject(ulong oid, uint tableId, string tableName, uint version);
        bool StartField(string name);
        bool SimpleField(object content);
        bool EndField();
        bool VisitFieldText(string name, string content);
        void VisitOidField(string name, ulong oid);
        bool StartDictionary(string name);
        bool StartDictKey();
        void EndDictKey();
        bool StartDictValue();
        void EndDictionary();
        void EndObject();
    }
}