using System;
using System.Diagnostics.SymbolStore;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILDynamicMethodDebugImpl : IILDynamicMethod
    {
        readonly Type _delegateType;
        int _expectedLength;
        IILGen _gen;
        AssemblyBuilder _assemblyBuilder;
        ModuleBuilder _moduleBuilder;
        ISymbolDocumentWriter _symbolDocumentWriter;
        TypeBuilder _typeBuilder;
        MethodBuilder _dynamicMethod;

        public ILDynamicMethodDebugImpl(string name, Type delegateType)
        {
            _delegateType = delegateType;
            _expectedLength = 64;
            var mi = delegateType.GetMethod("Invoke");

            _assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name + "Asm"), AssemblyBuilderAccess.RunAndSave);
            _moduleBuilder = _assemblyBuilder.DefineDynamicModule(name + "Asm.dll", true);
            _symbolDocumentWriter = _moduleBuilder.DefineDocument("dynamicIL/" + name + "Asm.il", SymDocumentType.Text, SymLanguageType.ILAssembly,SymLanguageVendor.Microsoft);
            _typeBuilder = _moduleBuilder.DefineType(name + "Impl", TypeAttributes.Public, typeof(object), Type.EmptyTypes);
            _dynamicMethod = _typeBuilder.DefineMethod("Invoke", MethodAttributes.Public | MethodAttributes.Static, mi.ReturnType, mi.GetParameters().Select(pi => pi.ParameterType).ToArray());
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenImpl(_dynamicMethod.GetILGenerator(_expectedLength))); }
        }

        public object Create()
        {
            var finalType = _typeBuilder.CreateType();
            _assemblyBuilder.Save("dynamicIL/"+_moduleBuilder.ScopeName);
            return Delegate.CreateDelegate(_delegateType, finalType, "Invoke");
        }
    }

    internal class ILDynamicMethodDebugImpl<T> : IILDynamicMethod<T> where T : class
    {
        int _expectedLength;
        IILGen _gen;
        readonly DynamicMethod _dynamicMethod;

        public ILDynamicMethodDebugImpl(string name)
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

        public T Create()
        {
            return _dynamicMethod.CreateDelegate<T>();
        }
    }
}