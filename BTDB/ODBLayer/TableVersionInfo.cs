using System;

namespace BTDB.ODBLayer
{
    internal class TableVersionInfo
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

        internal void Save(AbstractBufferedWriter writer)
        {
            writer.WriteVUInt32((uint)FieldCount);
            for (int i = 0; i < FieldCount; i++)
            {
                this[i].Save(writer);
            }
        }

        internal static TableVersionInfo Load(AbstractBufferedReader reader)
        {
            var fieldCount = reader.ReadVUInt32();
            var fieldInfos = new TableFieldInfo[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                fieldInfos[i] = TableFieldInfo.Load(reader);
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
    }
}