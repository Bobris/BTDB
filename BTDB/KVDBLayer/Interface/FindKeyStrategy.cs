namespace BTDB.KVDBLayer
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