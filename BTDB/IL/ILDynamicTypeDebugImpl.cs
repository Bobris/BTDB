using System;
using System.Collections.Concurrent;
using System.Diagnostics.SymbolStore;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILDynamicTypeDebugImpl : IILDynamicType
    {
        static readonly ConcurrentDictionary<string, int> UniqueNames = new ConcurrentDictionary<string, int>();

        readonly string _name;
        readonly AssemblyBuilder _assemblyBuilder;
        readonly ModuleBuilder _moduleBuilder;
        readonly ISymbolDocumentWriter _symbolDocumentWriter;
        readonly SourceCodeWriter _sourceCodeWriter;
        readonly TypeBuilder _typeBuilder;
        bool _inScope;
        readonly IILGenForbidenInstructions _forbidenInstructions;

        public ILDynamicTypeDebugImpl(string name, Type baseType, Type[] interfaces)
        {
            _name = name;
            var uniqueName = UniqueName(name);
            _assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(uniqueName), AssemblyBuilderAccess.RunAndSave, "dynamicIL");
            _moduleBuilder = _assemblyBuilder.DefineDynamicModule(uniqueName + ".dll", true);
            var sourceCodeFileName = Path.GetFullPath("dynamicIL/" + uniqueName + ".il");
            _symbolDocumentWriter = _moduleBuilder.DefineDocument(sourceCodeFileName, SymDocumentType.Text, SymLanguageType.ILAssembly, SymLanguageVendor.Microsoft);
            _sourceCodeWriter = new SourceCodeWriter(sourceCodeFileName, _symbolDocumentWriter);
            _sourceCodeWriter.WriteLine(string.Format("class {0} : {1}{2}", name, baseType.ToSimpleName(), string.Concat(interfaces.Select(i => ", " + i.ToSimpleName()))));
            _sourceCodeWriter.OpenScope();
            _typeBuilder = _moduleBuilder.DefineType(name, TypeAttributes.Public, baseType, interfaces);
            _forbidenInstructions = new ILGenForbidenInstructionsCheating(_typeBuilder);
        }

        internal static string UniqueName(string name)
        {
            name = name.Replace('<', '_');
            name = name.Replace('>', '_');
            name = name.Replace(',', '_');
            var uniqueName = name;
            var uniqueIdx = UniqueNames.AddOrUpdate(name, 0, (s, v) => v + 1);
            if (uniqueIdx != 0) uniqueName = string.Format("{0}__{1}", name, uniqueIdx);
            return uniqueName;
        }

        public IILMethod DefineMethod(string name, Type returns, Type[] parameters, MethodAttributes methodAttributes = MethodAttributes.Public)
        {
            CloseInScope();
            _sourceCodeWriter.StartMethod(name, returns, parameters, methodAttributes);
            var methodBuilder = _typeBuilder.DefineMethod(name, methodAttributes, returns, parameters);
            for (int i = 0; i < parameters.Length; i++)
            {
                methodBuilder.DefineParameter(i + 1, ParameterAttributes.In, string.Format("arg{0}", i));
            }
            _inScope = true;
            return new ILMethodDebugImpl(methodBuilder, _sourceCodeWriter, name, returns, parameters, _forbidenInstructions);
        }

        public FieldBuilder DefineField(string name, Type type, FieldAttributes fieldAttributes)
        {
            CloseInScope();
            _sourceCodeWriter.WriteLine(string.Format("{0} {1};", type.ToSimpleName(), name));
            return _typeBuilder.DefineField(name, type, fieldAttributes);
        }

        public IILEvent DefineEvent(string name, EventAttributes eventAttributes, Type type)
        {
            CloseInScope();
            _sourceCodeWriter.WriteLine(string.Format("event {0} {1};", type.ToSimpleName(), name));
            return new ILEventDebugImpl(_typeBuilder.DefineEvent(name, eventAttributes, type));
        }

        public IILMethod DefineConstructor(Type[] parameters)
        {
            CloseInScope();
            _sourceCodeWriter.StartMethod(_name, null, parameters, MethodAttributes.Public);
            _inScope = true;
            return new ILConstructorDebugImpl(_typeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, parameters), _forbidenInstructions, _sourceCodeWriter);
        }

        void CloseInScope()
        {
            if (!_inScope) return;
            _sourceCodeWriter.CloseScope();
            _sourceCodeWriter.WriteLine("");
            _inScope = false;
        }

        public void DefineMethodOverride(IILMethod methodBuilder, MethodInfo baseMethod)
        {
            _typeBuilder.DefineMethodOverride(((ILMethodDebugImpl)methodBuilder).MethodInfo, baseMethod);
        }

        public Type CreateType()
        {
            CloseInScope();
            _sourceCodeWriter.CloseScope();
            _sourceCodeWriter.Dispose();
            var finalType = _typeBuilder.CreateType();
            _forbidenInstructions.FinishType(finalType);
            _assemblyBuilder.Save(_moduleBuilder.ScopeName);
            return finalType;
        }
    }
}