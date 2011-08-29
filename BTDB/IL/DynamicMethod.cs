using System;
using System.Linq;
using System.Reflection.Emit;

namespace BTDB.IL
{
    public class DynamicMethod<T> where T : class
    {
        readonly DynamicMethod _dynamicMethod;

        public DynamicMethod(string name)
        {
            var t = typeof (T);
            if (!t.IsSubclassOf(typeof(Delegate))) throw new ArgumentException("Generic paramater T must be Delegate");
            var mi = t.GetMethod("Invoke");
            _dynamicMethod = new DynamicMethod(name, mi.ReturnType,
                                               mi.GetParameters().Select(pi => pi.ParameterType).ToArray());
        }

        public ILGenerator GetILGenerator()
        {
            return _dynamicMethod.GetILGenerator();
        }

        public ILGenerator GetILGenerator(int streamSize)
        {
            return _dynamicMethod.GetILGenerator(streamSize);
        }

        public T Create()
        {
            return _dynamicMethod.CreateDelegate<T>();
        }
    }
}