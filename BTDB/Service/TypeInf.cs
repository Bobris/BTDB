using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public class TypeInf
    {
        readonly string _name;
        readonly MethodInf[] _methodInfs;
        readonly PropertyInf[] _propertyInfs;
        readonly Type _type;

        public TypeInf(Type type, IFieldHandlerFactory fieldHandlerFactory)
        {
            _type = type;
            _name = type.Name;
            var methodInfs = new List<MethodInf>();
            var propertyInfs = new List<PropertyInf>();
            if (type.IsSubclassOf(typeof(Delegate)))
            {
                var method = type.GetMethod("Invoke");
                if (IsMethodSupported(method, fieldHandlerFactory))
                {
                    methodInfs.Add(new MethodInf(method, fieldHandlerFactory));
                }
            }
            else
            {
                var methods = type.GetMethods();
                foreach (var method in methods)
                {
                    var methodBase = method.GetBaseDefinition();
                    if (methodBase.DeclaringType == typeof(object)) continue;
                    if (methodBase.GetBaseDefinition().DeclaringType == typeof(IDisposable)) continue;
                    if (!methodBase.IsPublic) continue;
                    if (!IsMethodSupported(method, fieldHandlerFactory)) continue;
                    methodInfs.Add(new MethodInf(method, fieldHandlerFactory));
                }
                var properties = type.GetProperties();
                foreach (var property in properties)
                {
                    if (!property.CanRead || !property.CanWrite) continue;
                    if (property.GetCustomAttributes(typeof(NotStoredAttribute), true).Length != 0) return;
                    if (property.GetIndexParameters().Length != 0) continue;
                    if (property.GetGetMethod()==null) continue;
                    if (property.GetSetMethod()==null) continue;
                    if (!fieldHandlerFactory.TypeSupported(property.PropertyType)) continue;
                    propertyInfs.Add(new PropertyInf(property, fieldHandlerFactory));
                }
            }
            _methodInfs = methodInfs.ToArray();
            _propertyInfs = propertyInfs.ToArray();
        }

        public TypeInf(AbstractBufferedReader reader, IFieldHandlerFactory fieldHandlerFactory)
        {
            _type = null;
            _name = reader.ReadString();
            var methodCount = reader.ReadVUInt32();
            _methodInfs = new MethodInf[methodCount];
            for (int i = 0; i < methodCount; i++)
            {
                _methodInfs[i] = new MethodInf(reader, fieldHandlerFactory);
            }
            var properyCount = reader.ReadVUInt32();
            _propertyInfs = new PropertyInf[properyCount];
            for (int i = 0; i < properyCount; i++)
            {
                PropertyInfs[i] = new PropertyInf(reader, fieldHandlerFactory);
            }
        }

        public Type OriginalType
        {
            get { return _type; }
        }

        public string Name
        {
            get { return _name; }
        }

        public MethodInf[] MethodInfs
        {
            get { return _methodInfs; }
        }

        public PropertyInf[] PropertyInfs
        {
            get { return _propertyInfs; }
        }

        public void Store(AbstractBufferedWriter writer)
        {
            writer.WriteString(_name);
            writer.WriteVUInt32((uint)_methodInfs.Length);
            foreach (var methodInf in _methodInfs)
            {
                methodInf.Store(writer);
            }
            writer.WriteVUInt32((uint)PropertyInfs.Length);
            foreach (var propertyInf in PropertyInfs)
            {
                propertyInf.Store(writer);
            }
        }

        public IEnumerable<IFieldHandler> EnumerateFieldHandlers()
        {
            return
                _methodInfs.SelectMany(methodInf => methodInf.EnumerateFieldHandlers())
                .Concat(_propertyInfs.Select(propertyInf => propertyInf.FieldHandler));
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