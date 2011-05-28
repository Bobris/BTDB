namespace BTDB.KVDBLayer.Interface
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