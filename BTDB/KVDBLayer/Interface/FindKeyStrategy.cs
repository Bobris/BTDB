namespace BTDB.KVDBLayer.Interface
{
    public enum FindKeyStrategy
    {
        Create,
        ExactMatch,
        PreferPrevious,
        PreferNext,
        OnlyPrevious,
        OnlyNext
    }
}