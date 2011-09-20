using System.Linq;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public class TableVersionInfo
    {
        readonly TableFieldInfo[] _tableFields;

        internal TableVersionInfo(TableFieldInfo[] tableFields)
        {
            _tableFields = tableFields;
        }

        internal int FieldCount { get { return _tableFields.Length; } }

        internal TableFieldInfo this[int idx]
        {
            get { return _tableFields[idx]; }
        }

        internal TableFieldInfo this[string name]
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
                fieldInfos[i] = TableFieldInfo.Load(reader, fieldHandlerFactory, tableName);
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
    }
}