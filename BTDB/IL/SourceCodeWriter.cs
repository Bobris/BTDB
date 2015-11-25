using System;
using System.Diagnostics.SymbolStore;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;

namespace BTDB.IL
{
    class SourceCodeWriter : IDisposable
    {
        readonly string _fileName;
        readonly ISymbolDocumentWriter _symbolDocumentWriter;
        readonly StringBuilder _stringBuilder;
        readonly TextWriter _sourceWriter;
        int _currentLine;

        internal SourceCodeWriter(string fileName, ISymbolDocumentWriter symbolDocumentWriter)
        {
            var directoryName = Path.GetDirectoryName(fileName);
            if (!string.IsNullOrEmpty(directoryName))
                Directory.CreateDirectory(directoryName);
            _stringBuilder = new StringBuilder();
            _sourceWriter = new StringWriter(_stringBuilder);
            _fileName = fileName;
            _symbolDocumentWriter = symbolDocumentWriter;
            _currentLine = 1;
            Indent = 0;
        }

        internal int Indent { get; set; }

        int RealIndent => Indent * 4;

        internal void WriteLine(string text)
        {
            _sourceWriter.Write(new string(' ', RealIndent));
            _sourceWriter.WriteLine(text);
            _currentLine++;
        }

        internal void OpenScope()
        {
            WriteLine("{");
            Indent++;
        }

        internal void CloseScope()
        {
            Indent--;
            WriteLine("}");
        }

        internal void MarkAndWriteLine(ILGenerator il, string text)
        {
            il.MarkSequencePoint(_symbolDocumentWriter, _currentLine, 1 + RealIndent, _currentLine, text.Length + 1 + RealIndent);
            WriteLine(text);
        }

        public void Dispose()
        {
            _sourceWriter.Flush();
            _sourceWriter.Dispose();
            var newSource = _stringBuilder.ToString();
            string oldSource = null;
            if (File.Exists(_fileName))
            {
                oldSource = File.ReadAllText(_fileName);
            }
            if (newSource != oldSource)
            {
                File.WriteAllText(_fileName, newSource);
            }
        }

        internal void StartMethod(string name, MethodInfo mi)
        {
            StartMethod(name, mi.ReturnType, mi.GetParameters().Select(p => p.ParameterType).ToArray(), MethodAttributes.Static);
        }

        internal void StartMethod(string name, Type returns, Type[] parameters, MethodAttributes methodAttributes)
        {
            var before = "";
            var firstArgIdx = 1;
            if (methodAttributes.HasFlag(MethodAttributes.Public)) before += "public ";
            if (methodAttributes.HasFlag(MethodAttributes.Static))
            {
                firstArgIdx = 0;
                before += "static ";
            }
            if (methodAttributes.HasFlag(MethodAttributes.Virtual)) before += "virtual ";

            switch (parameters.Length)
            {
                case 0:
                    WriteLine(String.Format("{2}{0} {1}()", returns.ToSimpleName(), name, before));
                    break;
                case 1:
                    WriteLine(String.Format("{2}{0} {1}({3} arg{4})", returns.ToSimpleName(), name, before, parameters[0].ToSimpleName(), firstArgIdx));
                    break;
                default:
                    WriteLine(String.Format("{2}{0} {1}(", returns.ToSimpleName(), name, before));
                    Indent++;
                    int idx = 0;
                    foreach (var par in parameters)
                    {
                        WriteLine(
                            $"{par.ToSimpleName()} arg{idx + firstArgIdx}{(idx + 1 == parameters.Length ? ')' : ',')}");
                        idx++;
                    }
                    Indent--;
                    break;
            }
            OpenScope();
        }
    }
}