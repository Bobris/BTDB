using System;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public interface ITypeSerializersId2LoaderMapping
    {
        Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, object> GetLoader(uint typeId);
    }
}