using System;
using BTDB.ODBLayer;

namespace BTDB.Serialization;

public class CollectionMetadata
{
    public Type Type;
    public Type ElementKeyType;
    public Type? ElementValueType;
    public uint OffsetNext;
    public uint OffsetKey;
    public uint OffsetValue;
    public uint SizeOfEntry;
    public unsafe delegate*<uint, object> Creator;
    public unsafe delegate*<object, ref byte, void> Adder;
    public unsafe delegate*<object, ref byte, ref byte, void> AdderKeyValue;
    public unsafe delegate*<IInternalObjectDBTransaction, ODBDictionaryConfiguration, ulong, object> ODBCreator;
}
