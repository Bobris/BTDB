using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.SymbolStore;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;

namespace BTDB.IL
{
    internal class ILDynamicMethodDebugImpl : IILDynamicMethod, IILDynamicMethodWithThis
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
        readonly IILGenForbidenInstructions _forbidenInstructions;

        public ILDynamicMethodDebugImpl(string name, Type delegateType, Type thisType)
        {
            _delegateType = delegateType;
            _expectedLength = 64;
            var mi = delegateType.GetMethod("Invoke");
            var uniqueName = ILDynamicTypeDebugImpl.UniqueName(name);
            _assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(uniqueName), AssemblyBuilderAccess.RunAndSave, "dynamicIL");
            _moduleBuilder = _assemblyBuilder.DefineDynamicModule(uniqueName + ".dll", true);
            var sourceCodeFileName = Path.GetFullPath("dynamicIL/" + uniqueName + ".il");
            _symbolDocumentWriter = _moduleBuilder.DefineDocument(sourceCodeFileName, SymDocumentType.Text, SymLanguageType.ILAssembly, SymLanguageVendor.Microsoft);
            _sourceCodeWriter = new SourceCodeWriter(sourceCodeFileName, _symbolDocumentWriter);
            Type[] parameterTypes;
            if (thisType != null)
            {
                parameterTypes = new[] { thisType }.Concat(mi.GetParameters().Select(pi => pi.ParameterType)).ToArray();
            }
            else
            {
                parameterTypes = mi.GetParameters().Select(pi => pi.ParameterType).ToArray();
            }
            _sourceCodeWriter.StartMethod(name, mi.ReturnType, parameterTypes, MethodAttributes.Static);
            _typeBuilder = _moduleBuilder.DefineType(name, TypeAttributes.Public, typeof(object), Type.EmptyTypes);
            _forbidenInstructions = new ILGenForbidenInstructionsCheating(_typeBuilder);
            _dynamicMethod = _typeBuilder.DefineMethod("Invoke", MethodAttributes.Public | MethodAttributes.Static, mi.ReturnType, parameterTypes);
            for (int i = 0; i < parameterTypes.Length; i++)
            {
                _dynamicMethod.DefineParameter(i + 1, ParameterAttributes.In, string.Format("arg{0}", i));
            }
        }

        public void ExpectedLength(int length)
        {
            _expectedLength = length;
        }

        public IILGen Generator
        {
            get { return _gen ?? (_gen = new ILGenDebugImpl(_dynamicMethod.GetILGenerator(_expectedLength), _forbidenInstructions, _sourceCodeWriter)); }
        }

        public object Create(object @this)
        {
            var finalType = FinishType();
            return Delegate.CreateDelegate(_delegateType, @this, finalType.GetMethod("Invoke"));
        }

        public object Create()
        {
            var finalType = FinishType();
            return Delegate.CreateDelegate(_delegateType, finalType, "Invoke");
        }

        Type FinishType()
        {
            var finalType = _typeBuilder.CreateType();
            _forbidenInstructions.FinishType(finalType);
            _assemblyBuilder.Save(_moduleBuilder.ScopeName);
            _sourceCodeWriter.CloseScope();
            _sourceCodeWriter.Dispose();
            //CheckInPeVerify();
            return finalType;
        }

        void CheckInPeVerify()
        {
            const string peverifyPath =
                @"c:\Program Files (x86)\Microsoft SDKs\Windows\v8.0A\bin\NETFX 4.0 Tools\PEVerify.exe";
            if (!File.Exists(peverifyPath)) return;
            var fullyQualifiedName = _moduleBuilder.FullyQualifiedName;
            var nameInExeDirectory = Path.Combine(Path.GetDirectoryName(Path.GetDirectoryName(fullyQualifiedName)), Path.GetFileName(fullyQualifiedName));
            File.Copy(fullyQualifiedName, nameInExeDirectory, true);
            var additionalDlls = new List<string>(Assembly.ReflectionOnlyLoadFrom(nameInExeDirectory).GetReferencedAssemblies().Select(a => a.Name + ".dll").Where(a => File.Exists("dynamicIL/" + a)));
            additionalDlls.ForEach(s => File.Copy("dynamicIL/" + s, s));
            var psi = new ProcessStartInfo(peverifyPath, nameInExeDirectory)
                {
                    CreateNoWindow = true,
                    RedirectStandardOutput = true,
                    UseShellExecute = false
                };
            using (var p = new Process())
            {
                p.StartInfo = psi;
                var output = new StringBuilder();
                p.OutputDataReceived += (sender, args) => output.AppendLine(args.Data);
                p.Start();
                p.BeginOutputReadLine();
                p.WaitForExit();
                if (p.ExitCode != 0)
                {
                    output.Append("PEVerify ended with " + p.ExitCode + " for " + fullyQualifiedName);
                    var text = output.ToString();
                    Trace.WriteLine(text);
                    //Debugger.Break();
                }
            }
            File.Delete(nameInExeDirectory);
            additionalDlls.ForEach(File.Delete);
        }
    }

    internal class ILDynamicMethodDebugImpl<TDelegate> : ILDynamicMethodDebugImpl, IILDynamicMethod<TDelegate> where TDelegate : class
    {
        public ILDynamicMethodDebugImpl(string name)
            : base(name, typeof(TDelegate), null)
        {
        }

        public new TDelegate Create()
        {
            return (TDelegate)base.Create();
        }
    }
}