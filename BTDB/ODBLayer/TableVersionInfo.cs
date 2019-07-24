using System.Linq;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class TableVersionInfo
    {
        readonly TableFieldInfo[] _tableFields;

        internal TableVersionInfo(TableFieldInfo[] tableFields)
        {
            _tableFields = tableFields;
        }

        public int FieldCount => _tableFields.Length;

        public TableFieldInfo this[int idx] => _tableFields[idx];

        public TableFieldInfo this[string name]
        {
            get { return _tableFields.FirstOrDefault(tfi => tfi.Name == name); }
        }

        internal void Save(AbstractBufferedWriter writer)
        {
            writer.WriteVUInt32((uint)FieldCount);
            for (int i = 0; i < FieldCount; i++)
            {
                this[i].Save(writer);
            }
        }

        internal static TableVersionInfo Load(AbstractBufferedReader reader, IFieldHandlerFactory fieldHandlerFactory, string tableName)
        {
            var fieldCount = reader.ReadVUInt32();
            var fieldInfos = new TableFieldInfo[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                fieldInfos[i] = TableFieldInfo.Load(reader, fieldHandlerFactory, tableName, FieldHandlerOptions.None);
            }
            return new TableVersionInfo(fieldInfos);
        }

        internal static bool Equal(TableVersionInfo a, TableVersionInfo b)
        {
            if (a.FieldCount != b.FieldCount) return false;
            for (int i = 0; i < a.FieldCount; i++)
            {
                if (!TableFieldInfo.Equal(a[i], b[i])) return false;
            }
            return true;
        }

        internal bool NeedsCtx()
        {
            return _tableFields.Any(tfi => tfi.Handler.NeedsCtx());
        }

        internal bool NeedsInit()
        {
            return _tableFields.Any(tfi => tfi.Handler is IFieldHandlerWithInit);
        }
    }
}