using System;
using System.Diagnostics.SymbolStore;
using System.IO;
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
        readonly AssemblyBuilder _assemblyBuilder;
        readonly ModuleBuilder _moduleBuilder;
        readonly ISymbolDocumentWriter _symbolDocumentWriter;
        readonly TypeBuilder _typeBuilder;
        readonly MethodBuilder _dynamicMethod;
        readonly SourceCodeWriter _sourceCodeWriter;

        public ILDynamicMethodDebugImpl(string name, Type delegateType)
        {
            _delegateType = delegateType;
            _expectedLength = 64;
            var mi = delegateType.GetMethod("Invoke");
            _assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name + "Asm"), AssemblyBuilderAccess.RunAndSave, "dynamicIL");
            _moduleBuilder = _assemblyBuilder.DefineDynamicModule(name + "Asm.dll", true);
            var sourceCodeFileName = Path.GetFullPath("dynamicIL/" + name + "Asm.il");
            _symbolDocumentWriter = _moduleBuilder.DefineDocument(sourceCodeFileName, SymDocumentType.Text, SymLanguageType.ILAssembly, SymLanguageVendor.Microsoft);
            _sourceCodeWriter = new SourceCodeWriter(sourceCodeFileName, _symbolDocumentWriter);
            _sourceCodeWriter.StartMethod(name, mi);
            _typeBuilder = _moduleBuilder.DefineType(name + "Impl", TypeAttributes.Public, typeof(object), Type.EmptyTypes);
            _dynamicMethod = _typeBuilder.DefineMethod("Invoke", MethodAttributes.Public | MethodAttributes.Static, mi.ReturnType, mi.GetParameters().Select(pi => pi.ParameterType).ToArray());
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenDebugImpl(_dynamicMethod.GetILGenerator(_expectedLength), _sourceCodeWriter)); }
        }

        public MethodInfo MethodInfo
        {
            get { throw new InvalidOperationException(); }
        }

        public object Create()
        {
            var finalType = _typeBuilder.CreateType();
            _assemblyBuilder.Save(_moduleBuilder.ScopeName);
            _sourceCodeWriter.CloseScope();
            _sourceCodeWriter.Dispose();
            return Delegate.CreateDelegate(_delegateType, finalType, "Invoke");
        }
    }

    internal class ILDynamicMethodDebugImpl<T> : IILDynamicMethod<T> where T : class
    {
        int _expectedLength;
        IILGen _gen;
        readonly AssemblyBuilder _assemblyBuilder;
        readonly ModuleBuilder _moduleBuilder;
        readonly ISymbolDocumentWriter _symbolDocumentWriter;
        readonly TypeBuilder _typeBuilder;
        readonly MethodBuilder _dynamicMethod;
        readonly SourceCodeWriter _sourceCodeWriter;

        public ILDynamicMethodDebugImpl(string name)
        {
            _expectedLength = 64;
            var mi = typeof(T).GetMethod("Invoke");
            _assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name + "Asm"), AssemblyBuilderAccess.RunAndSave, "dynamicIL");
            _moduleBuilder = _assemblyBuilder.DefineDynamicModule(name + "Asm.dll", true);
            var sourceCodeFileName = Path.GetFullPath("dynamicIL/" + name + "Asm.il");
            _symbolDocumentWriter = _moduleBuilder.DefineDocument(sourceCodeFileName, SymDocumentType.Text, SymLanguageType.ILAssembly, SymLanguageVendor.Microsoft);
            _sourceCodeWriter = new SourceCodeWriter(sourceCodeFileName, _symbolDocumentWriter);
            _sourceCodeWriter.StartMethod(name, mi);
            _typeBuilder = _moduleBuilder.DefineType(name + "Impl", TypeAttributes.Public, typeof(object), Type.EmptyTypes);
            _dynamicMethod = _typeBuilder.DefineMethod("Invoke", MethodAttributes.Public | MethodAttributes.Static, mi.ReturnType, mi.GetParameters().Select(pi => pi.ParameterType).ToArray());
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenDebugImpl(_dynamicMethod.GetILGenerator(_expectedLength), _sourceCodeWriter)); }
        }

        public MethodInfo MethodInfo
        {
            get { throw new InvalidOperationException(); }
        }

        public T Create()
        {
            _sourceCodeWriter.CloseScope();
            _sourceCodeWriter.Dispose();
            var finalType = _typeBuilder.CreateType();
            _assemblyBuilder.Save(_moduleBuilder.ScopeName);
            return (T)(object)Delegate.CreateDelegate(typeof(T), finalType, "Invoke");
        }
    }
}