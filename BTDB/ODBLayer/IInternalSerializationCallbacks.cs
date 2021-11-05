using System;

namespace BTDB.ODBLayer;

public interface IInternalSerializationCallbacks
{
    void MetadataCreateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value);
    void CreateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value);
}
