using System;
using System.Diagnostics.SymbolStore;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.ODBLayer
{
    public class FieldHandlerCreateImpl
    {
        public TypeBuilder ImplType { get; set; }
        public ISymbolDocumentWriter SymbolDocWriter { get; set; }
        public string FieldName { get; set; }
        public ILGenerator Saver { get; set; }
        public PropertyInfo PropertyInfo { get; set; }
        public Action<ILGenerator> CallObjectModified { get; set; }
        public FieldBuilder FieldMidLevelDBTransaction { get; set; }
    }
}