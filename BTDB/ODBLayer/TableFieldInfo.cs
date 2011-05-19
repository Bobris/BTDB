using System;
using System.Reflection;
using BTDB.KVDBLayer;
using BTDB.KVDBLayer.ImplementationDetails;
using BTDB.KVDBLayer.Interface;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer
{
    public class TableFieldInfo
    {
        readonly string _name;
        readonly IFieldHandler _handler;

        TableFieldInfo(string name, IFieldHandler handler)
        {
            _name = name;
            _handler = handler;
        }

        internal string Name
        {
            get { return _name; }
        }

        internal IFieldHandler Handler
        {
            get { return _handler; }
        }

        internal static TableFieldInfo Load(AbstractBufferedReader reader, IFieldHandlerFactory fieldHandlerFactory, string tableName)
        {
            var name = reader.ReadString();
            var handlerName = reader.ReadString();
            var configuration = reader.ReadByteArray();
            var fieldHandler = fieldHandlerFactory.CreateFromName(tableName, name, handlerName, configuration);
            if (fieldHandler == null) throw new BTDBException(string.Format("FieldHandlerFactory did not created handler {0} in {1}.{2}", handlerName, tableName, name));
            return new TableFieldInfo(name, fieldHandler);
        }

        public static TableFieldInfo Build(string tableName, PropertyInfo pi, IFieldHandlerFactory fieldHandlerFactory, Type clientType)
        {
            var fieldHandler = fieldHandlerFactory.CreateFromProperty(tableName, clientType, pi);
            if (fieldHandler == null) throw new BTDBException(string.Format("FieldHandlerFactory did not build property {0} of type {2} in {1}", pi.Name, tableName, pi.PropertyType.FullName));
            return new TableFieldInfo(pi.Name, fieldHandler);
        }

        internal void Save(AbstractBufferedWriter writer)
        {
            writer.WriteString(_name);
            writer.WriteString(_handler.Name);
            writer.WriteByteArray(_handler.Configuration);
        }

        internal static bool Equal(TableFieldInfo a, TableFieldInfo b)
        {
            if (a.Name != b.Name) return false;
            var ha = a.Handler;
            var hb = b.Handler;
            if (ha == hb) return true;
            if (ha.Name != hb.Name) return false;
            var ca = ha.Configuration;
            var cb = hb.Configuration;
            if (ca == cb) return true;
            if (ca == null || cb == null) return false;
            if (ca.Length!=cb.Length) return false;
            if (BitArrayManipulation.CompareByteArray(ca, 0, ca.Length, cb, 0, cb.Length) != 0) return false;
            return true;
        }
    }
}