using System;
using System.Linq;
using System.Reflection.Emit;

namespace BTDB.IL
{
#if ILDEBUG
    using System.Diagnostics.SymbolStore;
    using System.Reflection;

    public class DynamicMethodSpecific
    {
        readonly Type _delegate;
        readonly MethodBuilder _dynamicMethod;
        readonly ISymbolDocumentWriter _symbolDocumentWriter;
        readonly AssemblyBuilder _assemblyBuilder;
        readonly ModuleBuilder _moduleBuilder;
        readonly TypeBuilder _typeBuilder;

        public DynamicMethodSpecific(string name, Type @delegate)
        {
            _delegate = @delegate;
            if (!@delegate.IsSubclassOf(typeof(Delegate))) throw new ArgumentException("Paramater @delegate must be Delegate");
            var mi = @delegate.GetMethod("Invoke");
            _assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name + "Asm"), AssemblyBuilderAccess.RunAndSave);
            _moduleBuilder = _assemblyBuilder.DefineDynamicModule(name + "Asm.dll", true);
            _symbolDocumentWriter = _moduleBuilder.DefineDocument("just_dynamic_" + name, Guid.Empty, Guid.Empty, Guid.Empty);
            _typeBuilder = _moduleBuilder.DefineType(name + "Impl", TypeAttributes.Public, typeof(object), Type.EmptyTypes);
            _dynamicMethod = _typeBuilder.DefineMethod("Invoke", MethodAttributes.Public | MethodAttributes.Static, mi.ReturnType, mi.GetParameters().Select(pi => pi.ParameterType).ToArray());
        }

        public ILGenerator GetILGenerator()
        {
            var ilGenerator = _dynamicMethod.GetILGenerator();
            ilGenerator.MarkSequencePoint(_symbolDocumentWriter, 1, 1, 1, 1);
            return ilGenerator;
        }

        public ILGenerator GetILGenerator(int streamSize)
        {
            var ilGenerator = _dynamicMethod.GetILGenerator(streamSize);
            ilGenerator.MarkSequencePoint(_symbolDocumentWriter, 1, 1, 1, 1);
            return ilGenerator;
        }

        public object Create()
        {
            var finalType = _typeBuilder.CreateType();
            _assemblyBuilder.Save(_moduleBuilder.ScopeName);
            return Delegate.CreateDelegate(_delegate, finalType, "Invoke");
        }
    }
#else
    public class DynamicMethodSpecific
    {
        readonly Type _delegate;
        readonly DynamicMethod _dynamicMethod;

        public DynamicMethodSpecific(string name, Type @delegate)
        {
            _delegate = @delegate;
            if (!@delegate.IsSubclassOf(typeof(Delegate))) throw new ArgumentException("Paramater @delegate must be Delegate");
            var mi = @delegate.GetMethod("Invoke");
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

        public object Create()
        {
            return _dynamicMethod.CreateDelegate(_delegate);
        }
    }
#endif
}