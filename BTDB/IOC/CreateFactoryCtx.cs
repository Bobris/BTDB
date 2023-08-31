using System;
using System.Collections.Generic;

namespace BTDB.IOC;

sealed class CreateFactoryCtx : ICreateFactoryCtx
{
    internal uint SingletonDeepness;
    internal bool VerifySingletons;
    internal bool Enumerate;

    readonly Dictionary<Type, int> _paramTypeToIndex = new();

    internal int GetParamSize()
    {
        return _paramTypeToIndex.Count;
    }

    internal bool HasResolvingCtx()
    {
        return GetParamSize()>0;
    }

    internal int AddInstanceToCtx(Type paramType)
    {
        if (_paramTypeToIndex.TryGetValue(paramType, out var idx)) return idx;
        idx = _paramTypeToIndex.Count;
        _paramTypeToIndex.Add(paramType, idx);
        return idx;
    }

    public bool IsBound(Type paramType, out int idx)
    {
        return _paramTypeToIndex.TryGetValue(paramType, out idx);
    }
}
