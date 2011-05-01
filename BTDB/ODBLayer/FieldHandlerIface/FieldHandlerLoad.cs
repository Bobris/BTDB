using System;
using System.Reflection.Emit;

namespace BTDB.ODBLayer
{
    public class FieldHandlerLoad
    {
        public string FieldName { get; set; }
        public Type ImplType { get; set; }
        public ILGenerator IlGenerator { get; set; }
        public Action<ILGenerator> PushThis { get; set; }
        public Action<ILGenerator> PushReader { get; set; }
        public TableVersionInfo ClientTableVersionInfo { get; set; }
        public TableFieldInfo TargetTableFieldInfo { get; set; }
    }
}