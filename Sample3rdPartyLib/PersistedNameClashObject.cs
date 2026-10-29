using BTDB.FieldHandler;

namespace Sample3rdPartyLib;

// Built without the source generator, so its clash of stored names is found only at runtime.
public class PersistedNameClashObject
{
    [PersistedName("X")] public int A { get; set; }
    public string? X { get; set; }
}
