using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.SymbolStore;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using BTDB.KVDBLayer;

namespace BTDB.IL
{
    class ILDynamicTypeDebugImpl : IILDynamicType
    {
        const int TypeLevelIndent = 1;

        static readonly ConcurrentDictionary<string, int> UniqueNames = new ConcurrentDictionary<string, int>();

        readonly string _name;
        readonly AssemblyBuilder _assemblyBuilder;
        readonly ModuleBuilder _moduleBuilder;
        readonly ISymbolDocumentWriter _symbolDocumentWriter;
        readonly SourceCodeWriter _sourceCodeWriter;
        readonly TypeBuilder _typeBuilder;
        readonly IILGenForbidenInstructions _forbidenInstructions;

        public ILDynamicTypeDebugImpl(string name, Type baseType, Type[] interfaces)
        {
            _name = name;
            var uniqueName = UniqueName(name, 259 - (DynamicILDirectoryPath.DynamicIL.Length + 1 + 4));
            _assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(uniqueName), AssemblyBuilderAccess.RunAndSave, DynamicILDirectoryPath.DynamicIL);
            _moduleBuilder = _assemblyBuilder.DefineDynamicModule(uniqueName + ".dll", true);
            var sourceCodeFileName = Path.Combine(DynamicILDirectoryPath.DynamicIL, uniqueName + ".il");
            _symbolDocumentWriter = _moduleBuilder.DefineDocument(sourceCodeFileName, SymDocumentType.Text, SymLanguageType.ILAssembly, SymLanguageVendor.Microsoft);
            _sourceCodeWriter = new SourceCodeWriter(sourceCodeFileName, _symbolDocumentWriter);
            _sourceCodeWriter.WriteLine(
                $"class {name} : {baseType.ToSimpleName()}{string.Concat(interfaces.Select(i => ", " + i.ToSimpleName()))}");
            _sourceCodeWriter.OpenScope();
            _typeBuilder = _moduleBuilder.DefineType(name, TypeAttributes.Public, baseType, interfaces);
            _forbidenInstructions = new ILGenForbidenInstructionsCheating(_typeBuilder);
        }

        internal static string UniqueName(string name, int maxLength = -1)
        {
            name = name.Replace('<', '_');
            name = name.Replace('>', '_');
            name = name.Replace(',', '_');
            name = name.Replace(" ", "");
            var uniqueName = maxLength == -1
                ? name
                : ShortenIfNeeded(name, maxLength - 5); //reserve 5 chars for __{uniqueIdx}
            var uniqueIdx = UniqueNames.AddOrUpdate(name, 0, (s, v) => v + 1);
            if (uniqueIdx != 0) uniqueName = $"{name}__{uniqueIdx}";
            return uniqueName;
        }

        internal static string ShortenIfNeeded(string name, int maxLength)
        {
            if (name.Length < maxLength)
                return name;
            var sections = ParseSectionsForNameShortening(name);
            int needRemove = name.Length - maxLength;
            SelectSubsectionsToRemove(name, sections, needRemove);
            var sb = new StringBuilder(maxLength);
            AddNameShorteningSection(name, sections, sections.Count - 1, name.Length, sb);
            return sb.ToString();
        }

        static void AddNameShorteningSection(string name, List<int> sections, int pos, int sectionEnd, StringBuilder sb)
        {
            var startIdx = sections[pos - sections[pos] - 1];
            var prevSectionPos = pos - sections[pos] - 2;
            if (prevSectionPos > 0)
                AddNameShorteningSection(name, sections, prevSectionPos, startIdx, sb);

            sb.Append(name[startIdx]);

            var skip = 0;
            for (var i = pos - sections[pos]; i < pos; i++)
                if (sections[i] < 0)
                    skip += -sections[i];
            for (var i = startIdx + 1 + skip; i < sectionEnd; i++)
                sb.Append(name[i]);
        }

        static void SelectSubsectionsToRemove(string name, List<int> sections, int needRemove)
        {   // subsections to remove mark as negative 0,0,|1,-2,2,2|7,-2,-2,2,|13,0
            var pos = sections.Count - 1;
            while (needRemove > 0)
            {
                var newPos = pos - sections[pos] - 2;
                for (var i = newPos + 2; i < pos; i++)
                {
                    if (sections[i] == 1)
                        continue;
                    needRemove -= sections[i];
                    sections[i] = -sections[i];
                    if (needRemove <= 0)
                        return;
                }
                if (newPos <=0)
                    throw new BTDBException($"Cannot shorten dynamic assembly path {name}.");
                pos = newPos;
            }
        }

        static List<int> ParseSectionsForNameShortening(string name)
        {
            //split string p_a.b.c_d.e.f_s into sections splitted by _
            // [pos of section start, [removable subsection, ...], removable subSections count]
            var parsed = new List<int> { 0 }; // 0,0,|1,2,2,2|7,2,2,2,|13,0
            var sectionCount = 0;
            var removableSectionStart = 0;
            for (int i = 0; i < name.Length; i++)
            {
                if (name[i] == '_')
                {
                    parsed.Add(sectionCount);
                    parsed.Add(i);
                    sectionCount = 0;
                    removableSectionStart = i + 1;
                }
                else if (name[i] == '.')
                {
                    parsed.Add(i - removableSectionStart + 1);
                    sectionCount++;
                    removableSectionStart = i + 1;
                }
            }
            parsed.Add(sectionCount);
            return parsed;
        }

        public IILMethod DefineMethod(string name, Type returns, Type[] parameters, MethodAttributes methodAttributes = MethodAttributes.Public)
        {
            CloseInScope();
            _sourceCodeWriter.StartMethod(name, returns, parameters, methodAttributes);
            var methodBuilder = _typeBuilder.DefineMethod(name, methodAttributes, returns, parameters);
            for (int i = 0; i < parameters.Length; i++)
            {
                methodBuilder.DefineParameter(i + 1, ParameterAttributes.In, $"arg{i}");
            }
            return new ILMethodDebugImpl(methodBuilder, _sourceCodeWriter, name, returns, parameters, _forbidenInstructions);
        }

        public IILField DefineField(string name, Type type, FieldAttributes fieldAttributes)
        {
            CloseInScope();
            _sourceCodeWriter.WriteLine($"{type.ToSimpleName()} {name};");
            return new ILFieldImpl(_typeBuilder.DefineField(name, type, fieldAttributes));
        }

        public IILEvent DefineEvent(string name, EventAttributes eventAttributes, Type type)
        {
            CloseInScope();
            _sourceCodeWriter.WriteLine($"event {type.ToSimpleName()} {name};");
            return new ILEventDebugImpl(_typeBuilder.DefineEvent(name, eventAttributes, type));
        }

        public IILMethod DefineConstructor(Type[] parameters)
        {
            CloseInScope();
            _sourceCodeWriter.StartMethod(_name, null, parameters, MethodAttributes.Public);
            return new ILConstructorDebugImpl(_typeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, parameters), _forbidenInstructions, _sourceCodeWriter);
        }

        void CloseInScope()
        {
            if (_sourceCodeWriter.Indent <= TypeLevelIndent)
                return;
            _sourceCodeWriter.CloseScope();
            _sourceCodeWriter.WriteLine("");
        }

        public void DefineMethodOverride(IILMethod methodBuilder, MethodInfo baseMethod)
        {
            _typeBuilder.DefineMethodOverride(((IILMethodPrivate)methodBuilder).TrueMethodInfo, baseMethod);
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

        public SourceCodeWriter TryGetSourceCodeWriter()
        {
            return _sourceCodeWriter;
        }
    }
}