using System;
using System.Diagnostics.SymbolStore;
using System.IO;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class SourceCodeWriter : IDisposable
    {
        readonly ISymbolDocumentWriter _symbolDocumentWriter;
        readonly StreamWriter _sourceWriter;
        int _currentLine;

        internal SourceCodeWriter(string fileName, ISymbolDocumentWriter symbolDocumentWriter)
        {
            var directoryName = Path.GetDirectoryName(fileName);
            if (!string.IsNullOrEmpty(directoryName))
                Directory.CreateDirectory(directoryName);
            _sourceWriter = new StreamWriter(fileName);
            _symbolDocumentWriter = symbolDocumentWriter;
            _currentLine = 1;
            Indent = 0;
        }

        internal int Indent { get; set; }

        int RealIndent { get { return Indent * 4; } }

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
        }

        internal void StartMethod(string name, MethodInfo mi)
        {
            var pars = mi.GetParameters();
            if (pars.Length == 0)
                WriteLine(String.Format("{0} {1}()", mi.ReturnType, name));
            else
            {
                WriteLine(String.Format("{0} {1}(", mi.ReturnType, name));
                Indent++;
                int idx = 0;
                foreach (var par in pars)
                {
                    WriteLine(String.Format("{0} arg{1}{2}", par.ParameterType.FullName, idx,
                                                             idx + 1 == pars.Length ? ')' : ','));
                    idx++;
                }
                Indent--;
            }
            OpenScope();
        }
    }
}