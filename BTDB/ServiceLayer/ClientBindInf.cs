using System;
using BTDB.KVDBLayer;

namespace BTDB.ServiceLayer
{
    public class ClientBindInf
    {
        internal uint BindingId { get; set; }
        internal uint ServiceId { get; set; }
        internal uint MethodId { get; set; }
        internal bool OneWay { get; set; }
        internal Action<object, AbstractBufferedReader> HandleResult { get; set; }
        internal Action<object, Exception> HandleException { get; set; }
        internal Func<TaskWithSource> TaskWithSourceCreator { get; set; }

        internal ClientBindInf() { }

        internal void Store(AbstractBufferedWriter writer)
        {
            writer.WriteVUInt32(BindingId);
            writer.WriteVUInt32(ServiceId);
            writer.WriteVUInt32(MethodId);
            writer.WriteBool(OneWay);
        }
    }
}