namespace ODbDump.Visitor
{
    class ToConsoleVisitorForComparison : ToConsoleVisitorNice
    {
        public override bool VisitSingleton(uint tableId, string tableName, ulong oid)
        {
            Print($"Singleton {tableName ?? "?Unknown?"}");
            return true;
        }

        public override bool StartObject(ulong oid, uint tableId, string tableName, uint version)
        {
            _indent++;
            Print($"Object {tableName ?? "?Unknown?"}");
            return true;
        }

        public override bool StartInlineObject(uint tableId, string tableName, uint version)
        {
            Print($"{_currentFieldName}: InlineObject {tableName} ref#{_iid}");
            _indent++;
            return true;
        }

        public override void OidReference(ulong oid)
        {
            Print($"{_currentFieldName}: OidReference");
        }
    }
}