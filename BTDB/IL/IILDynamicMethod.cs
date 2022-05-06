using System;

namespace BTDB.IL;

public interface IILDynamicMethod : IILMethod
{
    object Create();
}

public interface IILDynamicMethod<out T> : IILMethod where T : Delegate
{
    T Create();
}
