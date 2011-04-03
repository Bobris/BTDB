using System;

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

        internal static TableFieldInfo Load(AbstractBufferedReader reader)
        {
            var name = reader.ReadString();
            var type = reader.ReadVUInt32();
            return new TableFieldInfo(name,(FieldType) type);
        }

        internal void Save(AbstractBufferedWriter writer)
        {
            writer.WriteString(_name);
            writer.WriteVUInt32((uint) _type);
        }

        internal static bool Equal(TableFieldInfo a, TableFieldInfo b)
        {
            if (a.Name != b.Name) return false;
            if (a.Type != b.Type) return false;
            return true;
        }
    }
}