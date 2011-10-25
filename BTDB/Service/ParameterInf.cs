using System.Reflection;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public class ParameterInf
    {
        readonly string _name;
        readonly IFieldHandler _fieldHandler;

        public ParameterInf(ParameterInfo parameter, IFieldHandlerFactory fieldHandlerFactory)
        {
            _name = parameter.Name;
            _fieldHandler = fieldHandlerFactory.CreateFromType(parameter.ParameterType, FieldHandlerOptions.None);
        }

        public ParameterInf(AbstractBufferedReader reader, IFieldHandlerFactory fieldHandlerFactory)
        {
            _name = reader.ReadString();
            var handlerName = reader.ReadString();
            _fieldHandler = fieldHandlerFactory.CreateFromName(handlerName, reader.ReadByteArray());
        }

        public string Name
        {
            get { return _name; }
        }

        public IFieldHandler FieldHandler
        {
            get { return _fieldHandler; }
        }

        public void Store(AbstractBufferedWriter writer)
        {
            writer.WriteString(_name);
            writer.WriteString(_fieldHandler.Name);
            writer.WriteByteArray(_fieldHandler.Configuration);
        }
    }
}