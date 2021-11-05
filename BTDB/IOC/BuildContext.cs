using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.IL;
using BTDB.IOC.CRegs;

namespace BTDB.IOC;

class BuildContext : IBuildContext
{
    readonly ContainerImpl _container;
    BuildContext? _parent;
    List<KeyValuePair<ICRegMulti, int>>? _multiBinds;

    public BuildContext(ContainerImpl container)
    {
        _container = container;
    }

    ICReg? ChooseFromMulti(ICRegMulti multiRegistration, bool frozen)
    {
        if (_multiBinds != null)
        {
            var reg = _parent!.ChooseFromMulti(multiRegistration, true);
            if (reg != null) return reg;
            foreach (var multiBind in _multiBinds)
            {
                if (multiBind.Key == multiRegistration)
                {
                    return multiRegistration.Regs.Skip(multiBind.Value).First();
                }
            }
            if (!frozen)
            {
                _multiBinds.Add(new KeyValuePair<ICRegMulti, int>(multiRegistration, 0));
                return multiRegistration.Regs.First();
            }
        }
        return null;
    }

    IBuildContext MakeEnumEnabledChild()
    {
        return new BuildContext(_container)
        {
            _parent = this,
            _multiBinds = new List<KeyValuePair<ICRegMulti, int>>()
        };
    }

    static readonly Type[] TupleTypes = {
            typeof(Tuple<>),
            typeof(Tuple<,>),
            typeof(Tuple<,,>),
            typeof(Tuple<,,,>),
            typeof(Tuple<,,,,>),
            typeof(Tuple<,,,,,>),
            typeof(Tuple<,,,,,,>),
            typeof(Tuple<,,,,,,,>),
        };

    public ICRegILGen? ResolveNeedBy(Type type, object? key)
    {
        if (_container.Registrations.TryGetValue(new KeyAndType(key, type), out var registration))
        {
            if (registration is ICRegMulti multi)
            {
                registration = ChooseFromMulti(multi, false) ?? multi.ChosenOne;
            }
        }
        if (registration == null)
        {
            if (type.IsDelegate())
            {
                var resultType = type.GetMethod("Invoke")!.ReturnType;
                var nestedRegistration = ResolveNeedBy(resultType, key);
                if (nestedRegistration == null) return null;
                registration = new DelegateImpl(key, type, nestedRegistration);
            }
            else if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Lazy<>))
            {
                var resultType = type.GetGenericArguments()[0];
                var nestedRegistration = ResolveNeedBy(resultType, key);
                if (nestedRegistration == null) return null;
                registration = new LazyImpl(type, nestedRegistration);
            }
            else if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                var resultType = type.GetGenericArguments()[0];
                var child = MakeEnumEnabledChild();
                var nestedRegistration = child.ResolveNeedBy(resultType, key);
                if (nestedRegistration == null)
                {
                    if (key != null) return null;
                    registration = new EmptyEnumerableImpl(type, resultType);
                }
                else
                {
                    registration = new EnumerableImpl(key, type, resultType, child, nestedRegistration);
                }
            }
            else if (type.IsArray && type.GetArrayRank() == 1)
            {
                var resultType = type.GetElementType();
                var child = MakeEnumEnabledChild();
                var nestedRegistration = child.ResolveNeedBy(resultType!, key);
                if (nestedRegistration == null) return null;
                registration = new EnumerableImpl(key, type, resultType, child, nestedRegistration);
            }
            else if (type.IsGenericType && TupleTypes.Contains(type.GetGenericTypeDefinition()))
            {
                registration = new AlwaysNewImpl(type, type.GetConstructors()[0], false);
            }
        }
        if (registration != null)
        {
            if (registration is ICRegILGen result) return result;
            throw new ArgumentException("Builder for " + type.ToSimpleName() + " is not ILGen capable");
        }
        return null;
    }

    public IBuildContext? IncrementEnumerable()
    {
        if (_multiBinds == null) throw new InvalidOperationException("This context was not create by MakeEnumEnabledChild");
        var nextMultiBind = _multiBinds.ToList();
        while (nextMultiBind.Count > 0)
        {
            var pair = nextMultiBind[^1];
            pair = new KeyValuePair<ICRegMulti, int>(pair.Key, pair.Value + 1);
            if (pair.Value < pair.Key.Regs.Count())
            {
                nextMultiBind[^1] = pair;
                var child = new BuildContext(_container) { _parent = _parent, _multiBinds = nextMultiBind };
                return child;
            }
            nextMultiBind.RemoveAt(nextMultiBind.Count - 1);
        }
        return null;
    }

    public IBuildContext FreezeMulti()
    {
        if (_multiBinds == null) return this;
        return _parent!;
    }
}
