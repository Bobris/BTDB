using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace BTDB
{
    class DefaultTweaks : ITweaks
    {
        const int OptimumBTreeParentSize = 4096;
        const int OptimumBTreeChildSize = 4096;

        public bool ShouldSplitBTreeChild(int oldSize, int addSize, int oldKeys)
        {
            return oldSize + addSize > OptimumBTreeChildSize;
        }

        public bool ShouldSplitBTreeParent(int oldSize, int addSize, int oldChildren)
        {
            return oldSize + addSize > OptimumBTreeParentSize;
        }

        public ShouldMergeResult ShouldMergeBTreeParent(int lenPrevious, int lenCurrent, int lenNext)
        {
            if (lenPrevious < 0)
            {
                return lenCurrent + lenNext < OptimumBTreeParentSize ? ShouldMergeResult.MergeWithNext : ShouldMergeResult.NoMerge;
            }
            if (lenNext < 0)
            {
                return lenCurrent + lenPrevious < OptimumBTreeParentSize ? ShouldMergeResult.MergeWithPrevious : ShouldMergeResult.NoMerge;
            }
            if (lenPrevious < lenNext)
            {
                if (lenCurrent + lenPrevious < OptimumBTreeParentSize) return ShouldMergeResult.MergeWithPrevious;
            }
            else
            {
                if (lenCurrent + lenNext < OptimumBTreeParentSize) return ShouldMergeResult.MergeWithNext;
            }
            return ShouldMergeResult.NoMerge;
        }

        public bool ShouldMerge2BTreeChild(int leftCount, int leftLength, int rightCount, int rightLength)
        {
            if (leftLength + rightLength - 1 > OptimumBTreeChildSize) return false;
            return true;
        }

        public bool ShouldMerge2BTreeParent(int leftCount, int leftLength, int rightCount, int rightLength, int keyStorageLength)
        {
            if (leftLength + rightLength - 1 + keyStorageLength > OptimumBTreeParentSize) return false;
            return true;
        }

        public bool ShouldAttemptCacheCompaction(int sectorsInCache, int bytesInCache)
        {
            return sectorsInCache >= 9800;
        }

        public void WhichSectorsToRemoveFromCache(List<Sector> choosen)
        {
            foreach (var sector in choosen)
            {
                ulong price = sector.LastAccessTime;
                var s = sector.Parent;
                while (s != null)
                {
                    price++;
                    s.LastAccessTime = Math.Max(price, s.LastAccessTime);
                    s = s.Parent;
                }
            }
            choosen.Sort((a, b) => a.LastAccessTime < b.LastAccessTime ? -1 : (a.LastAccessTime > b.LastAccessTime ? 1 : 0));
            choosen.RemoveRange(choosen.Count / 2, choosen.Count - choosen.Count / 2);
        }

        public void NewSectorAddedToCache(Sector sector, int sectorsInCache, int bytesInCache)
        {
            Debug.Assert(sectorsInCache < 10000);
        }
    }
}