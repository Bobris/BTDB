using System;
using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC.CRegs;

class LazyImpl : ICReg, ICRegILGen
{
    readonly Type _type;
    readonly ICRegILGen _nestedRegistration;
    readonly Need _myNeed;

    public LazyImpl(Type type, ICRegILGen nestedRegistration)
    {
        _type = type;
        _nestedRegistration = nestedRegistration;
        _myNeed = new Need { Kind = NeedKind.Constant, ClrType = _type };
    }

    public string GenFuncName(IGenerationContext context)
    {
        return "Lazy_" + _type.ToSimpleName();
    }

    public void GenInitialization(IGenerationContext context)
    {
    }

    public bool IsCorruptingILStack(IGenerationContext context)
    {
        return false;
    }

    public IILLocal GenMain(IGenerationContext context)
    {
        context.PushToILStack(_myNeed);
        return null;
    }

    public IEnumerable<INeed> GetNeeds(IGenerationContext context)
    {
        if (_myNeed.Key == null)
        {
            var resultType = _type.GetGenericArguments()[0];
            _myNeed.Key = ((IObjectBuilder)
                    typeof(ClosureOfLazy<>).MakeGenericType(resultType).GetConstructors()[0].Invoke(new object[0])).Build(() =>
                        context.Container.BuildFromRegistration(_nestedRegistration, context.BuildContext)());
        }
        yield return _myNeed;
    }

    public bool IsSingletonSafe()
    {
        return true;
    }

    public interface IObjectBuilder
    {
        object Build(Func<object> builder);
    }

    public class ClosureOfLazy<T> : IObjectBuilder where T : class
    {
        public object Build(Func<object> builder)
        {
            return new Lazy<T>(() => (T)builder());
        }
    }

    public void Verify(ContainerVerification options, ContainerImpl container)
    {
    }
}
