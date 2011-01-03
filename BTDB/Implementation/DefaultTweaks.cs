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
            if (lenPrevious<0)
            {
                return lenCurrent + lenNext < OptimumBTreeParentSize ? ShouldMergeResult.MergeWithNext : ShouldMergeResult.NoMerge;
            }
            if (lenNext<0)
            {
                return lenCurrent + lenPrevious < OptimumBTreeParentSize ? ShouldMergeResult.MergeWithPrevious : ShouldMergeResult.NoMerge;
            }
            if (lenPrevious<lenNext)
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

        public void WhichSectorsToRemoveFromCache(List<KeyValuePair<Sector, ulong>> choosen)
        {
            var sectorMap = new Dictionary<long, int>(choosen.Count);
            for (int i = 0; i < choosen.Count; i++)
            {
                var sector = choosen[i].Key;
                sectorMap.Add(sector.Position, i);
            }
            for (int i = 0; i < choosen.Count; i++)
            {
                var sector = choosen[i].Key;
                ulong price = sector.LastAccessTime;
                choosen[i] = new KeyValuePair<Sector, ulong>(sector, Math.Max(price,choosen[i].Value));
                sector = sector.Parent;
                while (sector!=null)
                {
                    price++;
                    int index;
                    if (sectorMap.TryGetValue(sector.Position, out index))
                    {
                        choosen[index] = new KeyValuePair<Sector, ulong>(sector, Math.Max(price, choosen[index].Value));
                    }
                    sector = sector.Parent;
                }
            }
            choosen.Sort((a, b) => a.Value < b.Value ? -1 : (a.Value > b.Value ? 1: 0));
            choosen.RemoveRange(choosen.Count / 2, choosen.Count - choosen.Count / 2);
        }

        public void NewSectorAddedToCache(Sector sector, int sectorsInCache, int bytesInCache)
        {
            Debug.Assert(sectorsInCache < 10000);
        }
    }
}