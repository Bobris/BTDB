using System;

namespace BTDB
{
    class DefaultTweaks : ITweaks
    {
        public bool ShouldSplitBTreeChild(int oldSize, int addSize, int oldKeys)
        {
            return oldSize + addSize > 4096;
        }

        public bool ShouldSplitBTreeParent(int oldSize, int addSize, int oldChildren)
        {
            return oldSize + addSize > 4096;
        }

        public ShouldMergeResult ShouldMergeBTreeParent(int lenPrevious, int lenCurrent, int lenNext)
        {
            if (lenPrevious<0)
            {
                return lenCurrent + lenNext < 4096 ? ShouldMergeResult.MergeWithNext : ShouldMergeResult.NoMerge;
            }
            if (lenNext<0)
            {
                return lenCurrent + lenPrevious < 4096 ? ShouldMergeResult.MergeWithPrevious : ShouldMergeResult.NoMerge;
            }
            if (lenPrevious<lenNext)
            {
                if (lenCurrent + lenPrevious < 4096) return ShouldMergeResult.MergeWithPrevious;
            }
            else
            {
                if (lenCurrent + lenNext < 4096) return ShouldMergeResult.MergeWithNext;
            }
            return ShouldMergeResult.NoMerge;
        }

        public bool ShouldMerge2BTreeChild(int leftCount, int leftLength, int rightCount, int rightLength)
        {
            if (leftLength + rightLength - 1 > 4096) return false;
            return true;
        }

        public bool ShouldMerge2BTreeParent(int leftCount, int leftLength, int rightCount, int rightLength, int keyStorageLength)
        {
            if (leftLength + rightLength - 1 + keyStorageLength > 4096) return false;
            return true;
        }
    }
}