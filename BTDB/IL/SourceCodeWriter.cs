using System;
using System.Diagnostics.SymbolStore;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;

namespace BTDB.IL
{
    public class SourceCodeWriter : IDisposable
    {
        readonly string _fileName;
        readonly ISymbolDocumentWriter _symbolDocumentWriter;
        StringBuilder _stringBuilder;
        TextWriter _sourceWriter;
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
            if ((methodAttributes & MethodAttributes.Public) != 0) before += "public ";
            if ((methodAttributes & MethodAttributes.Static) != 0)
            {
                firstArgIdx = 0;
                before += "static ";
            }
            if ((methodAttributes & MethodAttributes.Virtual) != 0) before += "virtual ";

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

        public class StoredState
        {
            public StringBuilder StringBuilder;
            public TextWriter SourceWriter;
            public int CurrentLine;
            public int Indent;
        }

        public class InsertedBlock
        {
            public string Content { get; set; }
            public int IndentDiff { get; set; }
            public int AddedLinesCount { get; set; }
        }

        public StoredState ShelveBegin()
        {
            var state = new StoredState
            {
                SourceWriter = _sourceWriter,
                StringBuilder = _stringBuilder,
                CurrentLine = _currentLine,
                Indent = Indent
            };
            _stringBuilder = new StringBuilder();
            _sourceWriter = new StringWriter(_stringBuilder);
            return state;
        }

        public InsertedBlock ShelveEnd(StoredState state)
        {
            _sourceWriter.Flush();
            _sourceWriter.Dispose();
            var block = new InsertedBlock
            {
                Content = _stringBuilder.ToString(),
                IndentDiff = Indent - state.Indent,
                AddedLinesCount = _currentLine - state.CurrentLine
            };
            _sourceWriter = state.SourceWriter;
            _stringBuilder = state.StringBuilder;
            Indent = state.Indent;
            _currentLine = state.CurrentLine;
            return block;
        }

        public void InsertShelvedContent(InsertedBlock block)
        {
            _stringBuilder.Append(block.Content);
            _currentLine += block.AddedLinesCount;
            Indent += block.IndentDiff;
        }
    }
}