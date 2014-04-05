using System;
using System.Collections.Generic;

namespace BTDB.IL.Caching
{
    public class CachingILBuilder : IILBuilder
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

        public IILDynamicMethod<TDelegate> NewMethod<TDelegate>(string name) where TDelegate : class
        {
            var t = typeof(TDelegate);
            if (!t.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
            return new CachingILDynamicMethod<TDelegate>(this, name);
        }

        public IILDynamicMethodWithThis NewMethod(string name, Type @delegate, Type thisType)
        {
            if (thisType == null) throw new ArgumentNullException("thisType");
            if (!@delegate.IsDelegate()) throw new ArgumentException("Generic paramater T must be Delegate");
            return new CachingILDynamicMethodWithThis(this, name, @delegate, thisType);
        }

        public IILDynamicType NewType(string name, Type baseType, Type[] interfaces)
        {
            return Wrapping.NewType(name, baseType, interfaces);
        }

        public Type NewEnum(string name, Type baseType, IEnumerable<KeyValuePair<string, object>> literals)
        {
            return Wrapping.NewEnum(name, baseType, literals);
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
}