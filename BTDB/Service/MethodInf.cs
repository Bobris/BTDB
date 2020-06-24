using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.FieldHandler;
using BTDB.IL;

namespace BTDB.Service
{
    public class MethodInf
    {
        readonly string _name;
        readonly string _ifaceName;
        readonly ParameterInf[] _parameters;
        readonly IFieldHandler _resultFieldHandler;

        public MethodInf(MethodInfo method, IFieldHandlerFactory fieldHandlerFactory)
        {
            MethodInfo = method;
            _name = method.Name;
            foreach (var itf in GetInterfacesForMethod(method.DeclaringType, method.GetBaseDefinition()))
            {
                _ifaceName = itf.Name;
                break;
            }
            var syncReturnType = method.ReturnType.UnwrapTask();
            if (syncReturnType != typeof(void))
                _resultFieldHandler = fieldHandlerFactory.CreateFromType(syncReturnType, FieldHandlerOptions.None);
            var parameterInfos = method.GetParameters();
            _parameters = new ParameterInf[parameterInfos.Length];
            for (int i = 0; i < parameterInfos.Length; i++)
            {
                _parameters[i] = new ParameterInf(parameterInfos[i], fieldHandlerFactory);
            }
        }

        static IEnumerable<Type> GetInterfacesForMethod(Type type, MethodInfo method)
        {
            foreach (var itf in type.GetInterfaces())
            {
                var interfaceMap = type.GetInterfaceMap(itf);
                if (interfaceMap.TargetMethods.Any(m => m == method))
                {
                    yield return itf;
                }
            }
        }

        public MethodInf(AbstractBufferedReader reader, IFieldHandlerFactory fieldHandlerFactory)
        {
            _name = reader.ReadString();
            _ifaceName = reader.ReadString();
            var resultFieldHandlerName = reader.ReadString();
            if (resultFieldHandlerName != null)
            {
                _resultFieldHandler = fieldHandlerFactory.CreateFromName(resultFieldHandlerName, reader.ReadByteArray(), FieldHandlerOptions.None);
            }
            var parameterCount = reader.ReadVUInt32();
            _parameters = new ParameterInf[parameterCount];
            for (int i = 0; i < _parameters.Length; i++)
            {
                _parameters[i] = new ParameterInf(reader, fieldHandlerFactory);
            }
        }

        public string Name => _name;

        public string IfaceName => _ifaceName;

        public ParameterInf[] Parameters => _parameters;

        public IFieldHandler ResultFieldHandler => _resultFieldHandler;

        public MethodInfo MethodInfo { get; set; }

        public void Store(AbstractBufferedWriter writer)
        {
            writer.WriteString(_name);
            writer.WriteString(_ifaceName);
            if (_resultFieldHandler != null)
            {
                writer.WriteString(_resultFieldHandler.Name);
                writer.WriteByteArray(_resultFieldHandler.Configuration);
            }
            else
            {
                writer.WriteString(null);
            }
            writer.WriteVUInt32((uint)_parameters.Length);
            foreach (var parameter in _parameters)
            {
                parameter.Store(writer);
            }
        }

        public IEnumerable<IFieldHandler> EnumerateFieldHandlers()
        {
            if (_resultFieldHandler != null) yield return _resultFieldHandler;
            foreach (var parameterInf in _parameters)
            {
                yield return parameterInf.FieldHandler;
            }
        }
    }
}