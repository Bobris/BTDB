using System;
using System.Collections.Generic;
using System.Linq;

namespace BTDB.IL.Caching;

class CachingILBuilder : IILBuilder
{
    internal readonly IILBuilder Wrapping;
    internal readonly object Lock = new object();
    readonly Dictionary<object, object> _cache = new Dictionary<object, object>();

    public CachingILBuilder(IILBuilder wrapping)
    {
        Wrapping = wrapping;
    }

    public IILDynamicMethod NewMethod(string name, Type @delegate)
    {
        if (!@delegate.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
        return new CachingILDynamicMethod(this, name, @delegate);
    }

    public IILDynamicMethod<TDelegate> NewMethod<TDelegate>(string name) where TDelegate : Delegate
    {
        return new CachingILDynamicMethod<TDelegate>(this, name);
    }

    public IILDynamicMethodWithThis NewMethod(string name, Type @delegate, Type thisType)
    {
        if (thisType == null) throw new ArgumentNullException(nameof(thisType));
        if (!@delegate.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
        return new CachingILDynamicMethodWithThis(this, name, @delegate, thisType);
    }

    public IILDynamicType NewType(string name, Type baseType, Type[] interfaces)
    {
        return new CachingILDynamicType(this, name, baseType, interfaces);
    }

    public Type NewEnum(string name, Type baseType, IEnumerable<KeyValuePair<string, object>> literals)
    {
        lock (Lock)
        {
            var item = new EnumKey(name, baseType, literals);
            item = (EnumKey)FindInCache(item);
            return item.Result ?? (item.Result = Wrapping.NewEnum(name, baseType, item._literals));
        }
    }

    class EnumKey
    {
        readonly string _name;
        readonly Type _baseType;
        internal readonly KeyValuePair<string, object>[] _literals;
        internal Type Result;

        internal EnumKey(string name, Type baseType, IEnumerable<KeyValuePair<string, object>> literals)
        {
            _name = name;
            _baseType = baseType;
            _literals = literals.ToArray();
        }

        public override int GetHashCode()
        {
            return _name.GetHashCode() * 33 + _baseType.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            var v = obj as EnumKey;
            if (v == null) return false;
            return _name == v._name && _baseType == v._baseType && _literals.SequenceEqual(v._literals);
        }
    }

    public object FindInCache(object item)
    {
        object result;
        if (_cache.TryGetValue(item, out result))
        {
            return result;
        }
        _cache.Add(item, item);
        return item;
    }
}
