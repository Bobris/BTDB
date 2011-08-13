using System.Reflection;
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

        public string Name
        {
            get { return _name; }
        }

        public IFieldHandler FieldHandler
        {
            get { return _fieldHandler; }
        }
    }
}