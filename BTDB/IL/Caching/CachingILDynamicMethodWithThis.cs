using System;

namespace BTDB.IL.Caching
{
    class CachingILDynamicMethodWithThis : IILDynamicMethodWithThis
    {
        readonly CachingILBuilder _cachingILBuilder;
        readonly string _name;
        readonly Type _delegate;
        readonly Type _thisType;
        readonly CachingILGen _ilGen = new CachingILGen();
        int _expectedLength = -1;

        public CachingILDynamicMethodWithThis(CachingILBuilder cachingILBuilder, string name, Type @delegate, Type thisType)
        {
            _cachingILBuilder = cachingILBuilder;
            _name = name;
            _delegate = @delegate;
            _thisType = thisType;
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator => _ilGen;

        public void FinalizeCreation()
        {
        }

        public object Create(object @this)
        {
            lock (_cachingILBuilder.Lock)
            {
                var item = new CacheItem(_name, _delegate, _thisType, _ilGen);
                item = (CacheItem)_cachingILBuilder.FindInCache(item);
                if (item.Object == null)
                {
                    var method = _cachingILBuilder.Wrapping.NewMethod(_name, _delegate, _thisType);
                    if (_expectedLength != -1) method.ExpectedLength(_expectedLength);
                    _ilGen.ReplayTo(method.Generator);
                    method.FinalizeCreation();
                    item.Object = method.Create;
                }
                return item.Object(@this);
            }
        }

        internal class CacheItem
        {
            readonly string _name;
            readonly Type _delegate;
            readonly Type _thisType;
            readonly CachingILGen _ilGen;
            internal Func<object, object> Object;

            public CacheItem(string name, Type @delegate, Type thisType, CachingILGen ilGen)
            {
                _name = name;
                _delegate = @delegate;
                _thisType = thisType;
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
                return _name == v._name && _delegate == v._delegate && _thisType == v._thisType && _ilGen.Equals(v._ilGen);
            }
        }
    }
}