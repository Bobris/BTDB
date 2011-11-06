using System;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public class ClientBindInf
    {
        internal uint BindingId { get; set; }
        internal uint ServiceId { get; set; }
        internal uint MethodId { get; set; }
        internal bool OneWay { get; set; }
        internal Action<object, AbstractBufferedReader, IServiceInternalClient> HandleResult { get; set; }
        internal Action<object, Exception> HandleException { get; set; }
        internal Action<object> HandleCancellation { get; set; }
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