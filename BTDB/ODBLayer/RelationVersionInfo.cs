using System.Linq;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    class RelationVersionInfo
    {
        readonly TableFieldInfo[] _tableFields;

        public RelationVersionInfo(TableFieldInfo[] tableFieldInfo)
        {
            _tableFields = tableFieldInfo;
        }

        internal int FieldCount => _tableFields.Length;

        internal TableFieldInfo this[int idx] => _tableFields[idx];

        internal TableFieldInfo this[string name]
        {
            get { return _tableFields.FirstOrDefault(tfi => tfi.Name == name); }
        }

        public static RelationVersionInfo Load(KeyValueDBValueReader keyValueDbValueReader, IFieldHandlerFactory fieldHandlerFactory, string relationName)
        {
            throw new System.NotImplementedException();
        }

        internal bool NeedsCtx()
        {
            return _tableFields.Any(tfi => tfi.Handler.NeedsCtx());
        }

        internal bool NeedsInit()
        {
            return _tableFields.Any(tfi => tfi.Handler is IFieldHandlerWithInit);
        }

        internal static bool Equal(RelationVersionInfo a, RelationVersionInfo b)
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