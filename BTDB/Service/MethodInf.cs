using System.Collections.Generic;
using System.Reflection;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.StreamLayer;

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
            var methodBase = method.GetBaseDefinition();
            var syncReturnType = method.ReturnType.UnwrapTask();
            if (syncReturnType != typeof(void))
                _resultFieldHandler = fieldHandlerFactory.CreateFromType(syncReturnType, FieldHandlerOptions.None);
            if (methodBase != method) _ifaceName = methodBase.DeclaringType.Name;
            var parameterInfos = method.GetParameters();
            _parameters = new ParameterInf[parameterInfos.Length];
            for (int i = 0; i < parameterInfos.Length; i++)
            {
                _parameters[i] = new ParameterInf(parameterInfos[i], fieldHandlerFactory);
            }
        }

        public MethodInf(AbstractBufferedReader reader, IFieldHandlerFactory fieldHandlerFactory)
        {
            _name = reader.ReadString();
            _ifaceName = reader.ReadString();
            var resultFieldHandlerName = reader.ReadString();
            if (resultFieldHandlerName != null)
            {
                _resultFieldHandler = fieldHandlerFactory.CreateFromName(resultFieldHandlerName, reader.ReadByteArray());
            }
            var parameterCount = reader.ReadVUInt32();
            _parameters = new ParameterInf[parameterCount];
            for (int i = 0; i < _parameters.Length; i++)
            {
                _parameters[i] = new ParameterInf(reader, fieldHandlerFactory);
            }
        }

        public string Name
        {
            get { return _name; }
        }

        public string IfaceName
        {
            get { return _ifaceName; }
        }

        public ParameterInf[] Parameters
        {
            get { return _parameters; }
        }

        public IFieldHandler ResultFieldHandler
        {
            get { return _resultFieldHandler; }
        }

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