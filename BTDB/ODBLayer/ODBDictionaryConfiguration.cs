using System;
using System.Threading;
using BTDB.FieldHandler;

namespace BTDB.ODBLayer;

public class ODBDictionaryConfiguration
{
    static ODBDictionaryConfiguration[] _instances = Array.Empty<ODBDictionaryConfiguration>();

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
        if (!(KeyHandler.Configuration ?? Array.Empty<byte>()).AsSpan()
            .SequenceEqual((keyHandler.Configuration ?? Array.Empty<byte>()).AsSpan())) return false;
        if (valueHandler == ValueHandler) return true;
        if (valueHandler != null && ValueHandler == null) return false;
        if (valueHandler == null && ValueHandler != null) return false;
        if (valueHandler!.Name != ValueHandler.Name) return false;
        return (ValueHandler.Configuration ?? Array.Empty<byte>()).AsSpan()
            .SequenceEqual((valueHandler!.Configuration ?? Array.Empty<byte>()).AsSpan());
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
