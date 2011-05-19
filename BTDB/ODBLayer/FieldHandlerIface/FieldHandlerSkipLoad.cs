using System;
using System.Reflection.Emit;

namespace BTDB.ODBLayer.FieldHandlerIface
{
    public class FieldHandlerSkipLoad
    {
        public ILGenerator IlGenerator { get; set; }
        public Action<ILGenerator> PushReader { get; set; }
    }
}