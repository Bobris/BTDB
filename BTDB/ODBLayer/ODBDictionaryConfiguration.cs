using System;
using System.Threading;
using BTDB.FieldHandler;

namespace BTDB.ODBLayer
{
    public class ODBDictionaryConfiguration
    {
        static ODBDictionaryConfiguration[] _instances = Array.Empty<ODBDictionaryConfiguration>();

        internal static int Register(IFieldHandler keyHandler, IFieldHandler? valueHandler)
        {
            while (true)
            {
                var i = 0;
                var currentInstances = _instances;
                for (; i < currentInstances!.Length; i++)
                {
                    if (currentInstances[i].Compare(keyHandler, valueHandler))
                    {
                        return i;
                    }
                }

                i = currentInstances.Length;
                var newInstances = new ODBDictionaryConfiguration[i + 1];
                currentInstances.CopyTo(newInstances.AsSpan());
                newInstances[i] = new ODBDictionaryConfiguration(keyHandler, valueHandler);
                if (Interlocked.CompareExchange(ref _instances, newInstances, currentInstances) == currentInstances)
                    return i;
            }
        }

        internal static ODBDictionaryConfiguration Get(int index)
        {
            return _instances[index];
        }

        bool Compare(IFieldHandler keyHandler, IFieldHandler? valueHandler)
        {
            if (KeyHandler.Name != keyHandler.Name) return false;
            if (!(KeyHandler.Configuration ?? Array.Empty<byte>()).AsSpan()
                .SequenceEqual((keyHandler.Configuration ?? Array.Empty<byte>()).AsSpan())) return false;
            if (valueHandler == ValueHandler) return true;
            if (valueHandler != null && ValueHandler == null) return false;
            if (valueHandler == null && ValueHandler != null) return false;
            if (valueHandler!.Name != ValueHandler.Name) return false;
            return (ValueHandler.Configuration ?? Array.Empty<byte>()).AsSpan()
                .SequenceEqual((valueHandler!.Configuration ?? Array.Empty<byte>()).AsSpan());
        }

        ODBDictionaryConfiguration(IFieldHandler keyHandler, IFieldHandler? valueHandler)
        {
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

        internal static void Reset()
        {
            _instances = Array.Empty<ODBDictionaryConfiguration>();
        }
    }
}
