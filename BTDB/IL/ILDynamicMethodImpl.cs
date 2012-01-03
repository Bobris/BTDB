using System;
using System.Linq;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILDynamicMethodImpl : IILDynamicMethod, IILDynamicMethodWithThis
    {
        readonly Type _delegateType;
        int _expectedLength;
        IILGen _gen;
        readonly DynamicMethod _dynamicMethod;

        public ILDynamicMethodImpl(string name, Type delegateType, Type thisType)
        {
            _delegateType = delegateType;
            _expectedLength = 64;
            var mi = delegateType.GetMethod("Invoke");
            Type[] parameterTypes;
            if (thisType == null)
            {
                parameterTypes = mi.GetParameters().Select(pi => pi.ParameterType).ToArray();
            }
            else
            {
                parameterTypes = new[] { thisType }.Concat(mi.GetParameters().Select(pi => pi.ParameterType)).ToArray();
            }
            _dynamicMethod = new DynamicMethod(name, mi.ReturnType, parameterTypes, true);
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenImpl(_dynamicMethod.GetILGenerator(_expectedLength), new ILGenForbidenInstructionsGodPowers())); }
        }

        public object Create()
        {
            return _dynamicMethod.CreateDelegate(_delegateType);
        }

        public object Create(object @this)
        {
            return _dynamicMethod.CreateDelegate(_delegateType, @this);
        }
    }

    internal class ILDynamicMethodImpl<T> : IILDynamicMethod<T> where T : class
    {
        int _expectedLength;
        IILGen _gen;
        readonly DynamicMethod _dynamicMethod;

        public ILDynamicMethodImpl(string name)
        {
            _expectedLength = 64;
            var mi = typeof(T).GetMethod("Invoke");
            _dynamicMethod = new DynamicMethod(name, mi.ReturnType,
                                               mi.GetParameters().Select(pi => pi.ParameterType).ToArray(), true);
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenImpl(_dynamicMethod.GetILGenerator(_expectedLength), new ILGenForbidenInstructionsGodPowers())); }
        }

        public T Create()
        {
            return (T)(object)_dynamicMethod.CreateDelegate(typeof(T));
        }
    }
}