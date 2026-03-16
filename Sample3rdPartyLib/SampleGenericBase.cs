namespace Sample3rdPartyLib;

/// <summary>
/// Simulates a generic base class from an external assembly with auto-properties.
/// This pattern is common (e.g., CompanyMapKeyBase&lt;TKey&gt; in inspirecloud).
/// The BTDB source generator must handle auto-properties from external assemblies
/// correctly (using backing field access instead of UnsafeAccessor Method kind).
/// </summary>
public abstract class SampleGenericBase<TKey>
{
    public ulong CompanyId { get; set; }
    public TKey Id { get; set; } = default!;

    protected SampleGenericBase()
    {
    }

    protected SampleGenericBase(ulong companyId, TKey id)
    {
        CompanyId = companyId;
        Id = id;
    }
}
