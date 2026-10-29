using System;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

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

    /// The capacity to create a collection of count items read from data with. Each item takes at least a byte, so a
    /// damaged count, which past int.MaxValue would even be a negative capacity, only lets the collection grow as items
    /// arrive until the data ends.
    public static uint CapacityFor(uint count, in MemReader reader) =>
        Math.Min(count, reader.Controller == null ? (uint)Math.Max(0L, (long)(reader.End - reader.Current)) : 4096u);
}
