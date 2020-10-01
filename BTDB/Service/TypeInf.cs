using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.Collections;
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

        public TypeInf(Type type, IFieldHandlerFactory fieldHandlerFactory)
        {
            OriginalType = type;
            _name = type.Name;
            var methodInfs = new StructList<MethodInf>();
            var propertyInfs = new StructList<PropertyInf>();
            if (type.IsSubclassOf(typeof(Delegate)))
            {
                var method = type.GetMethod(nameof(Action.Invoke));
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
                var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
                foreach (var property in properties)
                {
                    if (!property.CanRead || !property.CanWrite) continue;
                    if (property.GetCustomAttributes(typeof(NotStoredAttribute), true).Length != 0) continue;
                    if (property.GetIndexParameters().Length != 0) continue;
                    if (property.GetAnyGetMethod()==null) continue;
                    if (property.GetAnySetMethod()==null) continue;
                    if (!fieldHandlerFactory.TypeSupported(property.PropertyType)) continue;
                    propertyInfs.Add(new PropertyInf(property, fieldHandlerFactory));
                }
            }
            _methodInfs = methodInfs.ToArray();
            _propertyInfs = propertyInfs.ToArray();
        }

        public TypeInf(AbstractBufferedReader reader, IFieldHandlerFactory fieldHandlerFactory)
        {
            OriginalType = null;
            _name = reader.ReadString()!;
            var methodCount = reader.ReadVUInt32();
            _methodInfs = new MethodInf[methodCount];
            for (var i = 0; i < methodCount; i++)
            {
                _methodInfs[i] = new MethodInf(reader, fieldHandlerFactory);
            }
            var propertyCount = reader.ReadVUInt32();
            _propertyInfs = new PropertyInf[propertyCount];
            for (var i = 0; i < propertyCount; i++)
            {
                PropertyInfs[i] = new PropertyInf(reader, fieldHandlerFactory);
            }
        }

        public Type? OriginalType { get; }

        public string Name => _name;

        public MethodInf[] MethodInfs => _methodInfs;

        public PropertyInf[] PropertyInfs => _propertyInfs;

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
