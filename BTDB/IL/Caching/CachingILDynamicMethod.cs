using System;

namespace BTDB.IL.Caching;

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
        InitLocals = true;
    }

    public void ExpectedLength(int length)
    {
        _expectedLength = length;
    }

    public bool InitLocals { get; set; }

    public IILGen Generator => _ilGen;

    public object Create()
    {
        lock (_cachingILBuilder.Lock)
        {
            var item = new CacheItem(_name, _delegate, _ilGen, InitLocals);
            item = (CacheItem)_cachingILBuilder.FindInCache(item);
            if (item.Object == null)
            {
                var method = _cachingILBuilder.Wrapping.NewMethod(_name, _delegate);
                if (_expectedLength != -1) method.ExpectedLength(_expectedLength);
                method.InitLocals = InitLocals;
                _ilGen.ReplayTo(method.Generator);
                _ilGen.FreeTemps();
                item.Object = method.Create();
            }
            return item.Object;
        }
    }

    class CacheItem
    {
        readonly string _name;
        readonly Type _delegate;
        readonly CachingILGen _ilGen;
        readonly bool _initLocals;
        internal object? Object;

        public CacheItem(string name, Type @delegate, CachingILGen ilGen, bool initLocals)
        {
            _name = name;
            _delegate = @delegate;
            _ilGen = ilGen;
            _initLocals = initLocals;
        }

        public override int GetHashCode()
        {
            return _name.GetHashCode() * 33 + _delegate.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            var v = obj as CacheItem;
            if (v == null) return false;
            return _name == v._name && _delegate == v._delegate && _ilGen.Equals(v._ilGen) && _initLocals == v._initLocals;
        }
    }
}

class CachingILDynamicMethod<TDelegate> : CachingILDynamicMethod, IILDynamicMethod<TDelegate> where TDelegate : Delegate
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
