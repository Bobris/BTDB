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
    }
}