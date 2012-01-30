using System;

namespace BTDB.IL.Caching
{
    class CachingILDynamicMethod : IILDynamicMethod
    {
        readonly CachingILBuilder _cachingILBuilder;
        readonly string _name;
        readonly Type _delegate;
        readonly CachingILGen _ilGen = new CachingILGen();
        int _expectedLength = -1;

        public CachingILDynamicMethod(CachingILBuilder cachingILBuilder, string name, Type @delegate)
        {
            _cachingILBuilder = cachingILBuilder;
            _name = name;
            _delegate = @delegate;
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _ilGen; }
        }

        public object Create()
        {
            lock (_cachingILBuilder.Lock)
            {
                var item = new CacheItem(_name, _delegate, _ilGen);
                item = (CacheItem)_cachingILBuilder.FindInCache(item);
                if (item.Object == null)
                {
                    var method = _cachingILBuilder.Wrapping.NewMethod(_name, _delegate);
                    if (_expectedLength != -1) method.ExpectedLength(_expectedLength);
                    _ilGen.ReplayTo(method.Generator);
                    item.Object = method.Create();
                }
                return item.Object;
            }
        }

        internal class CacheItem
        {
            readonly string _name;
            readonly Type _delegate;
            readonly CachingILGen _ilGen;
            internal object Object;

            public CacheItem(string name, Type @delegate, CachingILGen ilGen)
            {
                _name = name;
                _delegate = @delegate;
                _ilGen = ilGen;
            }

            public override int GetHashCode()
            {
                return _name.GetHashCode() * 33 + _delegate.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                var v = obj as CacheItem;
                if (v == null) return false;
                return _name == v._name && _delegate == v._delegate && _ilGen.Equals(v._ilGen);
            }
        }
    }

    class CachingILDynamicMethod<TDelegate> : CachingILDynamicMethod, IILDynamicMethod<TDelegate> where TDelegate : class
    {
        public CachingILDynamicMethod(CachingILBuilder cachingILBuilder, string name)
            : base(cachingILBuilder, name, typeof(TDelegate))
        {
        }

        public new TDelegate Create()
        {
            return (TDelegate)base.Create();
        }
    }
}
