using System;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    internal interface ITypeSerializersId2LoaderMapping
    {
        Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, object> GetLoader(uint typeId);
    }
}