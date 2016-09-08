using BTDB.FieldHandler;

namespace BTDB.ODBLayer
{
    public class ODBDictionaryConfiguration
    {
        readonly IObjectDB _odb;
        readonly IFieldHandler _keyHandler;
        readonly IFieldHandler _valueHandler;

        public ODBDictionaryConfiguration(IObjectDB odb, IFieldHandler keyHandler, IFieldHandler valueHandler)
        {
            _odb = odb;
            _keyHandler = keyHandler;
            _valueHandler = valueHandler;
        }

        public IFieldHandler KeyHandler => _keyHandler;

        public IFieldHandler ValueHandler => _valueHandler;

        public object KeyReader { get; set; }
        public object KeyWriter { get; set; }
        public object ValueReader { get; set; }
        public object ValueWriter { get; set; }

        public object FreeContent { get; set; }
    }
}