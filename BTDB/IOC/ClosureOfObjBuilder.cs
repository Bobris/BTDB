using System;

namespace BTDB.IOC
{
    internal static class ClosureOfObjBuilder
    {
        internal static object Build(Type funcType, object toReturn)
        {
            return Delegate.CreateDelegate(funcType,
                                           typeof(ClosureOfObj<>).MakeGenericType(funcType.GetGenericArguments()[0]).GetConstructors()[0].Invoke(new[] { toReturn }),
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
    }
}