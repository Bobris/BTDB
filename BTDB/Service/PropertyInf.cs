using System.Reflection;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public class PropertyInf
    {
        readonly string _name;
        readonly IFieldHandler _fieldHandler;

        public PropertyInf(PropertyInfo property, IFieldHandlerFactory fieldHandlerFactory)
        {
            _name = property.Name;
            _fieldHandler= fieldHandlerFactory.CreateFromType(property.PropertyType, FieldHandlerOptions.None);
        }

        public PropertyInf(ref SpanReader reader, IFieldHandlerFactory fieldHandlerFactory)
        {
            _name = reader.ReadString()!;
            var resultFieldHandlerName = reader.ReadString();
            _fieldHandler = fieldHandlerFactory.CreateFromName(resultFieldHandlerName!, reader.ReadByteArray(), FieldHandlerOptions.None);
        }

        public string Name => _name;

        public IFieldHandler FieldHandler => _fieldHandler;

        public void Store(ref SpanWriter writer)
        {
            writer.WriteString(_name);
            writer.WriteString(_fieldHandler.Name);
            writer.WriteByteArray(_fieldHandler.Configuration);
        }
    }
}
