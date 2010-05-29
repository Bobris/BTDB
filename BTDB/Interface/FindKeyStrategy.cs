namespace BTDB
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