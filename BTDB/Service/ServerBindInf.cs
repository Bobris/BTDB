using System;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public class ServerBindInf
    {
        internal uint BindingId { get; set; }
        internal uint ServiceId { get; set; }
        internal uint MethodId { get; set; }
        internal bool OneWay { get; set; }
        internal object Object { get; set; }

        internal delegate void RunnerFun(object that, ref SpanReader reader,
            IServiceInternalServer serviceInternalServer);

        internal RunnerFun Runner { get; set; }

        internal ServerBindInf(ref SpanReader reader)
        {
            BindingId = reader.ReadVUInt32();
            ServiceId = reader.ReadVUInt32();
            MethodId = reader.ReadVUInt32();
            OneWay = reader.ReadBool();
        }
    }
}
