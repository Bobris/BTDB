using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILDynamicMethodImpl: IILDynamicMethod
    {
        readonly Type _delegateType;
        int _expectedLength;
        IILGen _gen;
        readonly DynamicMethod _dynamicMethod;

        public ILDynamicMethodImpl(string name, Type delegateType)
        {
            _delegateType = delegateType;
            _expectedLength = 64;
            var mi = delegateType.GetMethod("Invoke");
            _dynamicMethod = new DynamicMethod(name, mi.ReturnType,
                                               mi.GetParameters().Select(pi => pi.ParameterType).ToArray());
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenImpl(_dynamicMethod.GetILGenerator(_expectedLength))); }
        }

        public MethodInfo MethodInfo
        {
            get { throw new InvalidOperationException(); }
        }

        public object Create()
        {
            return _dynamicMethod.CreateDelegate(_delegateType);
        }
    }

    internal class ILDynamicMethodImpl<T> : IILDynamicMethod<T> where T:class
    {
        int _expectedLength;
        IILGen _gen;
        readonly DynamicMethod _dynamicMethod;

        public ILDynamicMethodImpl(string name)
        {
            _expectedLength = 64;
            var mi = typeof(T).GetMethod("Invoke");
            _dynamicMethod = new DynamicMethod(name, mi.ReturnType,
                                               mi.GetParameters().Select(pi => pi.ParameterType).ToArray());
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenImpl(_dynamicMethod.GetILGenerator(_expectedLength))); }
        }

        public MethodInfo MethodInfo
        {
            get { throw new InvalidOperationException(); }
        }

        public T Create()
        {
            return (T)(object)_dynamicMethod.CreateDelegate(typeof(T));
        }
    }
}