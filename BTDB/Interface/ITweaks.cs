using System.Collections.Generic;

namespace BTDB
{
    public interface ITweaks
    {
        bool ShouldSplitBTreeChild(int oldSize, int addSize, int oldKeys);
        bool ShouldSplitBTreeParent(int oldSize, int addSize, int oldChildren);
        ShouldMergeResult ShouldMergeBTreeParent(int lenPrevious, int lenCurrent, int lenNext);
        bool ShouldMerge2BTreeChild(int leftCount, int leftLength, int rightCount, int rightLength);
        bool ShouldMerge2BTreeParent(int leftCount, int leftLength, int rightCount, int rightLength,
                                     int keyStorageLength);
        bool ShouldAttemptCacheCompaction(int sectorsInCache, int bytesInCache);
        void WhichSectorsToRemoveFromCache(List<KeyValuePair<Sector, int>> choosen);
        void NewSectorAddedToCache(Sector sector, int sectorsInCache, int bytesInCache);
    }
}