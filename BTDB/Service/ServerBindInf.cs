using System;

namespace BTDB.Service
{
    public class ServerBindInf
    {
        internal uint BindingId { get; set; }
        internal uint ServiceId { get; set; }
        internal uint MethodId { get; set; }
        internal bool OneWay { get; set; }
        internal object Object { get; set; }
        internal Action<object, AbstractBufferedReader, IServiceInternalServer> Runner { get; set; }

        internal ServerBindInf(AbstractBufferedReader reader)
        {
            BindingId = reader.ReadVUInt32();
            ServiceId = reader.ReadVUInt32();
            MethodId = reader.ReadVUInt32();
            OneWay = reader.ReadBool();
        }

    }
}