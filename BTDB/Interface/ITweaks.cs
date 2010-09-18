namespace BTDB
{
    public interface ITweaks
    {
        bool ShouldSplitBTreeChild(int oldSize, int addSize, int oldKeys);
        bool ShouldSplitBTreeParent(int oldSize, int addSize, int oldChildren);
        ShouldMergeResult ShouldMergeBTreeParent(int lenPrevious, int lenCurrent, int lenNext);
    }
}