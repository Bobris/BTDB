using System;

namespace BTDB.IOC;

static class ClosureOfObjBuilder
{
    internal static object Build(Type funcType, object toReturn)
    {
        var methodInfo = funcType.GetMethod("Invoke");
        var parameters = methodInfo.GetParameters();
        if (parameters.Length == 0)
            return Delegate.CreateDelegate(funcType,
                                           typeof(ClosureOfObj<>).MakeGenericType(methodInfo.ReturnType).GetConstructors()[0].Invoke(new[] { toReturn }),
                                           "Call");
        return Delegate.CreateDelegate(funcType,
                                        typeof(ClosureOfObj<,>).MakeGenericType(methodInfo.ReturnType, parameters[0].ParameterType).GetConstructors()[0].Invoke(new[] { toReturn }),
                                        "Call");
    }

    public class ClosureOfObj<T>
    {
        readonly T _obj;

        public ClosureOfObj(object obj)
        {
            _obj = (T)obj;
        }

        public T Call()
        {
            return _obj;
        }
    }

    public class ClosureOfObj<T, TP1>
    {
        readonly T _obj;

        public ClosureOfObj(object obj)
        {
            _obj = (T)obj;
        }

        public T Call(TP1 p1)
        {
            return _obj;
        }
    }
}
