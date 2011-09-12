using System;
using System.Collections.Generic;
using System.Reflection;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ServiceLayer
{
    public class TypeInf
    {
        readonly string _name;
        readonly MethodInf[] _methodInfs;

        public TypeInf(Type type, IFieldHandlerFactory fieldHandlerFactory)
        {
            _name = type.Name;
            var methodInfs = new List<MethodInf>();
            var methods = type.GetMethods();
            foreach (var method in methods)
            {
                var methodBase = method.GetBaseDefinition();
                if (methodBase.DeclaringType == typeof(object)) continue;
                if (!methodBase.IsPublic) continue;
                if (!IsMethodSupported(method, fieldHandlerFactory)) continue;
                methodInfs.Add(new MethodInf(method, fieldHandlerFactory));
            }
            _methodInfs = methodInfs.ToArray();
        }

        public TypeInf(AbstractBufferedReader reader, IFieldHandlerFactory fieldHandlerFactory)
        {
            _name = reader.ReadString();
            var methodCount = reader.ReadVUInt32();
            _methodInfs = new MethodInf[methodCount];
            for (int i = 0; i < methodCount; i++)
            {
                _methodInfs[i] = new MethodInf(reader, fieldHandlerFactory);
            }
        }

        public string Name
        {
            get { return _name; }
        }

        public MethodInf[] MethodInfs
        {
            get { return _methodInfs; }
        }

        public void Store(AbstractBufferedWriter writer)
        {
            writer.WriteString(_name);
            writer.WriteVUInt32((uint)_methodInfs.Length);
            foreach (var methodInf in _methodInfs)
            {
                methodInf.Store(writer);
            }
        }

        static bool IsMethodSupported(MethodInfo method, IFieldHandlerFactory fieldHandlerFactory)
        {
            var syncReturnType = method.ReturnType.UnwrapTask();
            if (syncReturnType != typeof(void))
                if (!fieldHandlerFactory.TypeSupported(syncReturnType)) return false;
            foreach (var parameter in method.GetParameters())
            {
                if (parameter.IsOptional) return false;
                if (parameter.IsOut) return false;
                if (parameter.IsRetval) return false;
                if (!fieldHandlerFactory.TypeSupported(parameter.ParameterType)) return false;
            }
            return true;
        }
    }
}