namespace BTDB.KVDBLayer
{
    public enum FindKeyResult
    {
        NotFound,
        FoundExact,
        FoundPrevious,
        FoundNext,
        Created
    }
}