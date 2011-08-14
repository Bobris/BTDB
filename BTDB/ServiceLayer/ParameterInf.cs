using System.Reflection;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;
using BTDB.ODBLayer.FieldHandlerImpl;

namespace BTDB.ServiceLayer
{
    public class ParameterInf
    {
        readonly string _name;
        readonly IFieldHandler _fieldHandler;

        public ParameterInf(ParameterInfo parameter)
        {
            _name = parameter.Name;
            _fieldHandler = new SignedFieldHandler();
        }

        public ParameterInf(AbstractBufferedReader reader)
        {
            _name = reader.ReadString();
            reader.ReadString();
            reader.ReadByteArray();
            _fieldHandler = new SignedFieldHandler();
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