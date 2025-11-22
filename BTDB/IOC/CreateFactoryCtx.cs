using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Collections;

namespace BTDB.IOC;

public sealed class CreateFactoryCtx : ICreateFactoryCtx
{
    internal uint SingletonDeepness;
    internal bool VerifySingletons;
    internal int Enumerate = -1;
    int _maxParams;

    Dictionary<(Type, string?), int> _paramTypeToIndex = new();

    readonly Dictionary<(Type type, int Enumerate, EquatableArray<(CReg, int)>),
        Func<IContainer, IResolvingCtx, object>> _lazyFactories = new();

    StructList<(CReg, int)> _enumeratingIndexes;
    StructList<CReg> _resolvingStack;
    StructList<int> _enumeratingStack;
    public bool ForbidKeylessFallback { get; set; }

    public int GetParamSize()
    {
        return _maxParams;
    }

    public bool HasResolvingCtx()
    {
        return _paramTypeToIndex.Count > 0;
    }

    public int AddInstanceToCtx(Type paramType, string? name = null)
    {
        if (_paramTypeToIndex.TryGetValue((paramType, name), out var idx)) return idx;
        idx = _paramTypeToIndex.Count;
        _paramTypeToIndex.Add((paramType, name), idx);
        _maxParams = Math.Max(_maxParams, idx + 1);
        return idx;
    }

    public bool IsBound(Type paramType, string? name, out int idx)
    {
        if (_paramTypeToIndex.TryGetValue((paramType, name), out idx)) return true;
        if (name != null)
        {
            if (_paramTypeToIndex.TryGetValue((paramType, null), out idx)) return true;
            var onlyOne = false;
            foreach (var ((item1, _), value) in _paramTypeToIndex)
            {
                if (item1 != paramType) continue;
                idx = value;
                if (onlyOne) return false;
                onlyOne = true;
            }

            return onlyOne;
        }

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
        if ((_enumeratingIndexes.Count > 0 || Enumerate >= 0) &&
            _lazyFactories.TryGetValue((type, Enumerate, new(_enumeratingIndexes.ToArray())), out factory))
            return true;
        // To make it easier Lazy factory could match global/root factory
        // In theory it should match also all parent enumerating scopes, not just root
        foreach (var ((type1, enumerate, immutableArray), value) in _lazyFactories)
        {
            if (type1 == type && enumerate == -1 && immutableArray.Count == 0)
            {
                factory = value;
                return true;
            }
        }

        factory = null;
        return false;
    }

    public void RegisterLazyFactory(Type type, Func<IContainer, IResolvingCtx?, object?> factory)
    {
        _lazyFactories.Add((type, Enumerate, new(_enumeratingIndexes.ToArray())), factory);
    }

    internal void PushResolving(CReg cReg)
    {
        foreach (var reg in _resolvingStack)
        {
            if (reg == cReg) throw new InvalidOperationException("Detected circular dependency");
        }

        _resolvingStack.Add(cReg);
        _enumeratingStack.Add(Enumerate);
        Enumerate = -1;
    }

    public void PopResolving()
    {
        _resolvingStack.RemoveAt(^1);
        Enumerate = _enumeratingStack[^1];
        _enumeratingStack.RemoveAt(^1);
    }

    internal StructList<CReg> BackupResolvingStack()
    {
        var res = _resolvingStack;
        _resolvingStack = new StructList<CReg>();
        return res;
    }

    internal void RestoreResolvingStack(StructList<CReg> backup)
    {
        _resolvingStack = backup;
    }

    internal CReg Enumerating(CReg cReg)
    {
        foreach (var (item1, item2) in _enumeratingIndexes)
        {
            if (item1 == cReg) return cReg.Multi[item2];
        }

        _enumeratingIndexes.Add((cReg, 0));
        return cReg.Multi[0];
    }

    public int StartEnumerate()
    {
        var res = Enumerate;
        Enumerate = (int)_enumeratingIndexes.Count;
        return res;
    }

    public bool IncrementEnumerable()
    {
        var i = (int)_enumeratingIndexes.Count - 1;
        while (i >= Enumerate)
        {
            var (cReg, idx) = _enumeratingIndexes[i];
            if (idx + 1 < cReg.Multi.Count)
            {
                _enumeratingIndexes[i] = (cReg, idx + 1);
                return true;
            }

            _enumeratingIndexes[i] = (cReg, 0);
            i--;
        }

        return false;
    }

    public void FinishEnumerate(int enumerableBackup)
    {
        while (_enumeratingIndexes.Count > Enumerate)
            _enumeratingIndexes.Pop();
        Enumerate = enumerableBackup;
    }

    readonly record struct CtxRestorer(CreateFactoryCtx Ctx, Dictionary<(Type, string?), int> ParamTypeToIndex)
        : IDisposable
    {
        public void Dispose()
        {
            Ctx._paramTypeToIndex = ParamTypeToIndex;
            if (ParamTypeToIndex.Count == 0) Ctx._maxParams = 0;
        }
    }

    public IDisposable ResolvingCtxRestorer()
    {
        return new CtxRestorer(this, _paramTypeToIndex.ToDictionary());
    }

    internal readonly record struct CtxEnumerableRestorer(
        CreateFactoryCtx Ctx,
        StructList<(CReg, int)> EnumeratingIndexes,
        int Enumerate)
        : IDisposable
    {
        public void Dispose()
        {
            Ctx._enumeratingIndexes = EnumeratingIndexes;
            Ctx.Enumerate = Enumerate;
        }
    }

    internal CtxEnumerableRestorer EnumerableRestorer()
    {
        var res = new CtxEnumerableRestorer(this, _enumeratingIndexes, Enumerate);
        _enumeratingIndexes = new();
        Enumerate = -1;
        return res;
    }
}
