using BTDB.ODBLayer;

namespace Sample3rdPartyLib;

public abstract class ExternalTeamVisibility<TItemId>
{
    [PrimaryKey(1)] public ulong CompanyId { get; set; }
    [PrimaryKey(2)] public ulong TeamId { get; set; }
    [PrimaryKey(3)]
    [SecondaryKey("Item", IncludePrimaryKeyOrder = 1)]
    public TItemId ItemId { get; set; } = default!;
}
