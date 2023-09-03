using System;
using System.Collections.Generic;
using BTDB.Collections;

namespace BTDB.IOC;

sealed class CreateFactoryCtx : ICreateFactoryCtx
{
    internal uint SingletonDeepness;
    internal bool VerifySingletons;
    internal bool Enumerate;

    readonly Dictionary<(Type, string?), int> _paramTypeToIndex = new();
    readonly Dictionary<Type, Func<IContainer, IResolvingCtx?, object?>> _lazyFactories = new();
    StructList<CReg> _resolvingStack;

    public int GetParamSize()
    {
        return _paramTypeToIndex.Count;
    }

    public bool HasResolvingCtx()
    {
        return GetParamSize() > 0;
    }

    public int AddInstanceToCtx(Type paramType, string? name = null)
    {
        if (_paramTypeToIndex.TryGetValue((paramType, name), out var idx)) return idx;
        idx = _paramTypeToIndex.Count;
        _paramTypeToIndex.Add((paramType, name), idx);
        return idx;
    }

    public bool IsBound(Type paramType, string? name, out int idx)
    {
        if (_paramTypeToIndex.TryGetValue((paramType, name), out idx)) return true;
        if (name != null) return _paramTypeToIndex.TryGetValue((paramType, null), out idx);
        foreach (var ((item1, _), value) in _paramTypeToIndex)
        {
            if (item1 != paramType) continue;
            idx = value;
            return true;
        }

        return false;
    }

    public bool GetLazyFactory(Type type, out Func<IContainer, IResolvingCtx?, object?>? factory)
    {
        return _lazyFactories.TryGetValue(type, out factory);
    }

    public void RegisterLazyFactory(Type type, Func<IContainer, IResolvingCtx?, object?> factory)
    {
        _lazyFactories.Add(type, factory);
    }

    public void PushResolving(CReg cReg)
    {
        foreach (var reg in _resolvingStack)
        {
            if (reg == cReg) throw new InvalidOperationException("Detected circular dependency");
        }

        _resolvingStack.Add(cReg);
    }

    public void PopResolving()
    {
        _resolvingStack.RemoveAt(^1);
    }

    public StructList<CReg> BackupResolvingStack()
    {
        var res = _resolvingStack;
        _resolvingStack = new StructList<CReg>();
        return res;
    }

    public void RestoreResolvingStack(StructList<CReg> backup)
    {
        _resolvingStack = backup;
    }
}
