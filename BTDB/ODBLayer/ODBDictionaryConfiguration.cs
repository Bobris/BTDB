using BTDB.FieldHandler;

namespace BTDB.ODBLayer
{
    public class ODBDictionaryConfiguration
    {
        readonly IObjectDB _odb;

        public ODBDictionaryConfiguration(IObjectDB odb, IFieldHandler keyHandler, IFieldHandler? valueHandler)
        {
            _odb = odb;
            KeyHandler = keyHandler;
            ValueHandler = valueHandler;
        }

        public IFieldHandler KeyHandler { get; }

        public IFieldHandler? ValueHandler { get; }

        public object? KeyReader { get; set; }
        public object? KeyWriter { get; set; }
        public object? ValueReader { get; set; }
        public object? ValueWriter { get; set; }

        public object? FreeContent { get; set; }
    }
}
