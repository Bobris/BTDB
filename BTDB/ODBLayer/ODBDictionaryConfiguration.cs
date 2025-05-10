using System;
using System.Threading;
using BTDB.FieldHandler;

namespace BTDB.ODBLayer;

public class ODBDictionaryConfiguration
{
    static ODBDictionaryConfiguration[] _instances = [];

    internal static int Register(IFieldHandler keyHandler, Type keyType, IFieldHandler? valueHandler, Type? valueType)
    {
        while (true)
        {
            var i = 0;
            var currentInstances = _instances;
            for (; i < currentInstances!.Length; i++)
            {
                if (currentInstances[i].Compare(keyHandler, keyType, valueHandler, valueType))
                {
                    return i;
                }
            }

            i = currentInstances.Length;
            var newInstances = new ODBDictionaryConfiguration[i + 1];
            currentInstances.CopyTo(newInstances.AsSpan());
            newInstances[i] = new ODBDictionaryConfiguration(keyHandler, keyType, valueHandler, valueType);
            if (Interlocked.CompareExchange(ref _instances, newInstances, currentInstances) == currentInstances)
                return i;
        }
    }

    internal static ODBDictionaryConfiguration Get(int index)
    {
        return _instances[index];
    }

    bool Compare(IFieldHandler keyHandler, Type keyType, IFieldHandler? valueHandler, Type? valueType)
    {
        if (keyType != KeyType) return false;
        if (valueType != ValueType) return false;
        if (KeyHandler.Name != keyHandler.Name) return false;
        if (!(KeyHandler.Configuration ?? []).AsSpan()
            .SequenceEqual((keyHandler.Configuration ?? []).AsSpan())) return false;
        if (valueHandler == ValueHandler) return true;
        if (valueHandler != null && ValueHandler == null) return false;
        if (valueHandler == null && ValueHandler != null) return false;
        if (valueHandler!.Name != ValueHandler.Name) return false;
        return (ValueHandler.Configuration ?? []).AsSpan()
            .SequenceEqual((valueHandler!.Configuration ?? []).AsSpan());
    }

    ODBDictionaryConfiguration(IFieldHandler keyHandler, Type keyType, IFieldHandler? valueHandler, Type? valueType)
    {
        KeyHandler = keyHandler;
        KeyType = keyType;
        ValueHandler = valueHandler;
        ValueType = valueType;
    }

    public Type KeyType { get; }
    public IFieldHandler KeyHandler { get; }

    public Type? ValueType { get; }
    public IFieldHandler? ValueHandler { get; }

    public RefReaderFun? KeyReader { get; set; }
    public RefWriterFun? KeyWriter { get; set; }
    public RefReaderFun? ValueReader { get; set; }
    public RefWriterFun? ValueWriter { get; set; }

    internal static void Reset()
    {
        _instances = [];
    }
}
