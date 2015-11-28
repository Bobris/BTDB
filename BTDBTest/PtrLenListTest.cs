using System.Text;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest
{
    public class PtrLenListTest
    {
        static string Str(PtrLenList l)
        {
            var sb = new StringBuilder();
            foreach (var kv in l)
            {
                sb.AppendFormat("{0}-{1};", kv.Key, kv.Key + kv.Value);
            }
            if (sb.Length > 0) sb.Length--;
            return sb.ToString();
        }

        [Fact]
        public void NewListIsEmpty()
        {
            var l = new PtrLenList();
            Assert.True(l.Empty);
        }

        [Fact]
        public void EmptyIncludeIsNoop()
        {
            var l = new PtrLenList();
            Assert.True(l.TryInclude(0, 0));
            Assert.True(l.Empty);
        }

        [Fact]
        public void AfterIncludeIsNonEmpty()
        {
            var l = new PtrLenList();
            Assert.True(l.TryInclude(0, 1));
            Assert.False(l.Empty);
        }

        [Fact]
        public void EmptyExcludeIsNoop()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            Assert.True(l.TryExclude(1, 0));
            Assert.Equal("0-2", Str(l));
        }

        [Fact]
        public void DoubleIncludeFails()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            Assert.False(l.TryInclude(0, 1));
        }

        [Fact]
        public void OnEmptyExcludeFails()
        {
            var l = new PtrLenList();
            Assert.False(l.TryExclude(0, 1));
        }

        [Fact]
        public void IncludeAndExcludeMakesEmpty()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            Assert.True(l.TryExclude(0, 1));
            Assert.True(l.Empty);
        }

        [Fact]
        public void TwoConnectedIncludesMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            Assert.True(l.TryInclude(1, 1));
            Assert.Equal("0-2", Str(l));
        }

        [Fact]
        public void TwoDisconnectedIncludesMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            Assert.True(l.TryInclude(2, 1));
            Assert.Equal("0-1;2-3", Str(l));
        }

        [Fact]
        public void ThreeIncludesMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            l.TryInclude(2, 1);
            Assert.True(l.TryInclude(1, 1));
            Assert.Equal("0-3", Str(l));
        }

        [Fact]
        public void ThreeMergeOverlaping1()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(4, 2);
            Assert.False(l.TryInclude(1, 3));
            Assert.Equal("0-6", Str(l));
        }

        [Fact]
        public void ThreeMergeOverlaping2()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(4, 2);
            Assert.False(l.TryInclude(2, 3));
            Assert.Equal("0-6", Str(l));
        }

        [Fact]
        public void ThreeMergeOverlaping3()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(4, 2);
            Assert.False(l.TryInclude(1, 4));
            Assert.Equal("0-6", Str(l));
        }

        [Fact]
        public void InsertAtBeginingWithoutMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(4, 2);
            l.TryInclude(8, 2);
            Assert.True(l.TryInclude(0, 2));
            Assert.Equal("0-2;4-6;8-10", Str(l));
        }

        [Fact]
        public void InsertAtBeginingWithMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(4, 2);
            l.TryInclude(8, 2);
            Assert.True(l.TryInclude(0, 4));
            Assert.Equal("0-6;8-10", Str(l));
        }

        [Fact]
        public void InsertBeforeLastWithMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(8, 2);
            Assert.False(l.TryInclude(4, 5));
            Assert.Equal("0-2;4-10", Str(l));
        }

        [Fact]
        public void ALotsOfIncludes()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 100; i++)
            {
                Assert.True(l.TryInclude(i * 4, 2));
            }
            Assert.False(l.TryInclude(5, 390));
            Assert.Equal("0-2;4-395;396-398", Str(l));
        }

        [Fact]
        public void ALotsOfIncludes2()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 100; i++)
            {
                Assert.True(l.TryInclude(i * 4, 2));
            }
            Assert.False(l.TryInclude(7, 388));
            Assert.Equal("0-2;4-6;7-395;396-398", Str(l));
        }

        [Fact]
        public void ExcludeFromStart()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 4);
            Assert.True(l.TryExclude(0, 2));
            Assert.Equal("2-4", Str(l));
        }

        [Fact]
        public void ExcludeFromStartOverlap()
        {
            var l = new PtrLenList();
            l.TryInclude(1, 3);
            Assert.False(l.TryExclude(0, 2));
            Assert.Equal("2-4", Str(l));
        }

        [Fact]
        public void ExcludeFromEnd()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 4);
            Assert.True(l.TryExclude(2, 2));
            Assert.Equal("0-2", Str(l));
        }

        [Fact]
        public void ExcludeFromEndOverlap()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 3);
            Assert.False(l.TryExclude(2, 2));
            Assert.Equal("0-2", Str(l));
        }

        [Fact]
        public void ExcludeSplitRange()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 10);
            Assert.True(l.TryExclude(3, 2));
            Assert.Equal("0-3;5-10", Str(l));
        }

        [Fact]
        public void SpanExclude()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 10; i++)
            {
                Assert.True(l.TryInclude(i * 4, 2));
            }
            Assert.False(l.TryExclude(5, 30));
            Assert.Equal("0-2;4-5;36-38", Str(l));
        }

        [Fact]
        public void SpanExclude2()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 10; i++)
            {
                Assert.True(l.TryInclude(i * 4, 2));
            }
            Assert.False(l.TryExclude(24, 10));
            Assert.Equal("0-2;4-6;8-10;12-14;16-18;20-22;36-38", Str(l));
        }

        [Fact]
        public void SpanExclude3()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 10; i++)
            {
                Assert.True(l.TryInclude(i * 4, 2));
            }
            Assert.False(l.TryExclude(24, 20));
            Assert.Equal("0-2;4-6;8-10;12-14;16-18;20-22", Str(l));
        }

        [Fact]
        public void SpanExclude4()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 10; i++)
            {
                Assert.True(l.TryInclude(i * 4, 2));
            }
            Assert.False(l.TryExclude(16, 17));
            Assert.Equal("0-2;4-6;8-10;12-14;33-34;36-38", Str(l));
        }

        [Fact]
        public void AfterClearIsEmpty()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            l.Clear();
            Assert.True(l.Empty);
        }

        [Fact]
        public void CloneIsEqual()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            l.TryInclude(10, 11);
            var l2 = l.Clone();
            Assert.Equal("0-1;10-21", Str(l2));
            Assert.Equal("0-1;10-21", Str(l));
        }

        [Fact]
        public void EmptyCloneIsEmpty()
        {
            var l = new PtrLenList();
            var l2 = l.Clone();
            Assert.True(l2.Empty);
        }

        [Fact]
        public void CloneAndClearWorks()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            l.TryInclude(10, 11);
            var l2 = l.CloneAndClear();
            Assert.Equal("0-1;10-21", Str(l2));
            Assert.True(l.Empty);
        }

        [Fact]
        public void EmptyCloneAndClearIsEmpty()
        {
            var l = new PtrLenList();
            var l2 = l.CloneAndClear();
            Assert.True(l2.Empty);
        }

        [Fact]
        public void TryFindAndRemoveWholeRange()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(5, 5);
            l.TryInclude(15, 10);
            ulong pos;
            Assert.False(l.TryFindLenAndRemove(15, out pos));
            Assert.Equal("0-2;5-10;15-25", Str(l));
            Assert.True(l.TryFindLenAndRemove(5, out pos));
            Assert.Equal(5ul, pos);
            Assert.Equal("0-2;15-25", Str(l));
            Assert.True(l.TryFindLenAndRemove(10, out pos));
            Assert.Equal(15ul, pos);
            Assert.Equal("0-2", Str(l));
            Assert.True(l.TryFindLenAndRemove(2, out pos));
            Assert.Equal(0ul, pos);
            Assert.Equal("", Str(l));
        }

        [Fact]
        public void TryFindAndRemovePartRange()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(5, 5);
            l.TryInclude(15, 10);
            ulong pos;
            Assert.True(l.TryFindLenAndRemove(3, out pos));
            Assert.Equal(5ul, pos);
            Assert.Equal("0-2;8-10;15-25", Str(l));
            Assert.True(l.TryFindLenAndRemove(3, out pos));
            Assert.Equal(15ul, pos);
            Assert.Equal("0-2;8-10;18-25", Str(l));
            Assert.True(l.TryFindLenAndRemove(1, out pos));
            Assert.Equal(0ul, pos);
            Assert.Equal("1-2;8-10;18-25", Str(l));
        }

        [Fact]
        public void UnmergeInPlaceWorks()
        {
            var l1 = new PtrLenList();
            l1.TryInclude(0, 2);
            l1.TryInclude(5, 5);
            l1.TryInclude(15, 10);
            var l2 = new PtrLenList();
            l2.TryInclude(6, 3);
            l2.TryInclude(15, 10);
            l1.UnmergeInPlace(l2);
            Assert.Equal("0-2;5-6;9-10", Str(l1));
        }

        [Fact]
        public void FindFreeSizeAfterWorks()
        {
            var l1 = new PtrLenList();
            l1.TryInclude(0, 2);
            l1.TryInclude(5, 5);
            l1.TryInclude(15, 10);
            Assert.Equal(25u, l1.FindFreeSizeAfter(1, 6));
            Assert.Equal(10u, l1.FindFreeSizeAfter(1, 4));
            Assert.Equal(2u, l1.FindFreeSizeAfter(1, 3));
            Assert.Equal(12u, l1.FindFreeSizeAfter(12, 1));
        }
    }
}
