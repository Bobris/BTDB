﻿using System.Reflection;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

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

        internal string Name => _name;

        internal IFieldHandler Handler => _handler;

        internal static TableFieldInfo Load(AbstractBufferedReader reader, IFieldHandlerFactory fieldHandlerFactory,
            string tableName, FieldHandlerOptions handlerOptions)
        {
            var name = reader.ReadString();
            var handlerName = reader.ReadString();
            var configuration = reader.ReadByteArray();
            var fieldHandler = fieldHandlerFactory.CreateFromName(handlerName, configuration, handlerOptions);
            if (fieldHandler == null) throw new BTDBException(
                $"FieldHandlerFactory did not created handler {handlerName} in {tableName}.{name}");
            return new TableFieldInfo(name, fieldHandler);
        }

        public static TableFieldInfo Build(string tableName, PropertyInfo pi, IFieldHandlerFactory fieldHandlerFactory,
              FieldHandlerOptions handlerOptions)
        {
            var fieldHandler = fieldHandlerFactory.CreateFromType(pi.PropertyType, handlerOptions);
            if (fieldHandler == null) throw new BTDBException(string.Format("FieldHandlerFactory did not build property {0} of type {2} in {1}", pi.Name, tableName, pi.PropertyType.FullName));
            var a = pi.GetCustomAttribute<PersistedNameAttribute>();
            return new TableFieldInfo(a != null ? a.Name : pi.Name, fieldHandler);
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
            if (ca.Length != cb.Length) return false;
            if (BitArrayManipulation.CompareByteArray(ca, ca.Length, cb, cb.Length) != 0) return false;
            return true;
        }
    }
}