namespace BTDB.KVDBLayer;

/// Constraint for a newly allocated file identity. Existing files are never renumbered.
public enum FileIdParity
{
    Any,
    Odd,
    Even
}
