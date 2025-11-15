using System;
using System.Runtime.CompilerServices;

namespace BTDB.Collections;

public readonly struct DerivedUtf16Utf8Dictionary<TDerivedValue, TValue>
{
    readonly Utf16Utf8Dictionary<TValue> _parent;
    readonly TDerivedValue[] _values;

    public DerivedUtf16Utf8Dictionary(Utf16Utf8Dictionary<TValue> parent)
    {
        _parent = parent;
        _values = new TDerivedValue[parent.Count];
    }

    public Utf16Utf8Dictionary<TValue> Parent => _parent;

    public ref TDerivedValue ValueRef(uint index)
    {
        return ref _values[index];
    }

    public ref TDerivedValue GetValueRef(ReadOnlySpan<char> key, out bool found)
    {
        var index = _parent.GetIndex(key);
        if (index >= 0)
        {
            found = true;
            return ref _values[(uint)index];
        }

        found = false;
        return ref Unsafe.NullRef<TDerivedValue>();
    }

    public ref TDerivedValue GetValueRef(ReadOnlySpan<byte> key, out bool found)
    {
        var index = _parent.GetIndex(key);
        if (index >= 0)
        {
            found = true;
            return ref _values[(uint)index];
        }

        found = false;
        return ref Unsafe.NullRef<TDerivedValue>();
    }

    public bool TryGetValue(ReadOnlySpan<char> key, out TDerivedValue value)
    {
        var index = _parent.GetIndex(key);
        if (index >= 0)
        {
            value = _values[(uint)index];
            return true;
        }

        value = default!;
        return false;
    }

    public bool TryGetValue(ReadOnlySpan<byte> key, out TDerivedValue value)
    {
        var index = _parent.GetIndex(key);
        if (index >= 0)
        {
            value = _values[(uint)index];
            return true;
        }

        value = default!;
        return false;
    }
}
