namespace BTDB.ODBLayer
{
    internal class TableFieldInfo
    {
        readonly string _name;
        readonly FieldType _type;

        internal TableFieldInfo(string name, FieldType type)
        {
            _name = name;
            _type = type;
        }

        internal string Name
        {
            get { return _name; }
        }

        internal FieldType Type
        {
            get { return _type; }
        }
    }
}