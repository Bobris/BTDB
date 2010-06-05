using System.Text;
using BTDB;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class PtrLenListTest
    {
        private static string Str(PtrLenList l)
        {
            var sb = new StringBuilder();
            foreach (var kv in l)
            {
                sb.AppendFormat("{0}-{1};", kv.Key, kv.Key + kv.Value);
            }
            if (sb.Length > 0) sb.Length--;
            return sb.ToString();
        }

        [Test]
        public void NewListIsEmpty()
        {
            var l = new PtrLenList();
            Assert.IsTrue(l.Empty);
        }

        [Test]
        public void EmptyIncludeIsNoop()
        {
            var l = new PtrLenList();
            Assert.IsTrue(l.TryInclude(0, 0));
            Assert.IsTrue(l.Empty);
        }

        [Test]
        public void AfterIncludeIsNonEmpty()
        {
            var l = new PtrLenList();
            Assert.IsTrue(l.TryInclude(0, 1));
            Assert.IsFalse(l.Empty);
        }

        [Test]
        public void EmptyExcludeIsNoop()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            Assert.IsTrue(l.TryExclude(1, 0));
            Assert.AreEqual("0-2", Str(l));
        }

        [Test]
        public void DoubleIncludeFails()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            Assert.IsFalse(l.TryInclude(0, 1));
        }

        [Test]
        public void OnEmptyExcludeFails()
        {
            var l = new PtrLenList();
            Assert.IsFalse(l.TryExclude(0, 1));
        }

        [Test]
        public void IncludeAndExcludeMakesEmpty()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            Assert.IsTrue(l.TryExclude(0, 1));
            Assert.IsTrue(l.Empty);
        }

        [Test]
        public void TwoConnectedIncludesMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            Assert.IsTrue(l.TryInclude(1, 1));
            Assert.AreEqual("0-2", Str(l));
        }

        [Test]
        public void TwoDisconnectedIncludesMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            Assert.IsTrue(l.TryInclude(2, 1));
            Assert.AreEqual("0-1;2-3", Str(l));
        }

        [Test]
        public void ThreeIncludesMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            l.TryInclude(2, 1);
            Assert.IsTrue(l.TryInclude(1, 1));
            Assert.AreEqual("0-3", Str(l));
        }

        [Test]
        public void ThreeMergeOverlaping1()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(4, 2);
            Assert.IsFalse(l.TryInclude(1, 3));
            Assert.AreEqual("0-6", Str(l));
        }

        [Test]
        public void ThreeMergeOverlaping2()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(4, 2);
            Assert.IsFalse(l.TryInclude(2, 3));
            Assert.AreEqual("0-6", Str(l));
        }

        [Test]
        public void ThreeMergeOverlaping3()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(4, 2);
            Assert.IsFalse(l.TryInclude(1, 4));
            Assert.AreEqual("0-6", Str(l));
        }

        [Test]
        public void InsertAtBeginingWithoutMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(4, 2);
            l.TryInclude(8, 2);
            Assert.IsTrue(l.TryInclude(0, 2));
            Assert.AreEqual("0-2;4-6;8-10", Str(l));
        }

        [Test]
        public void InsertAtBeginingWithMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(4, 2);
            l.TryInclude(8, 2);
            Assert.IsTrue(l.TryInclude(0, 4));
            Assert.AreEqual("0-6;8-10", Str(l));
        }

        [Test]
        public void InsertBeforeLastWithMerge()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(8, 2);
            Assert.IsFalse(l.TryInclude(4, 5));
            Assert.AreEqual("0-2;4-10", Str(l));
        }

        [Test]
        public void ALotsOfIncludes()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 100; i++)
            {
                Assert.IsTrue(l.TryInclude(i * 4, 2));
            }
            Assert.IsFalse(l.TryInclude(5, 390));
            Assert.AreEqual("0-2;4-395;396-398", Str(l));
        }

        [Test]
        public void ALotsOfIncludes2()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 100; i++)
            {
                Assert.IsTrue(l.TryInclude(i * 4, 2));
            }
            Assert.IsFalse(l.TryInclude(7, 388));
            Assert.AreEqual("0-2;4-6;7-395;396-398", Str(l));
        }

        [Test]
        public void ExcludeFromStart()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 4);
            Assert.IsTrue(l.TryExclude(0, 2));
            Assert.AreEqual("2-4", Str(l));
        }

        [Test]
        public void ExcludeFromStartOverlap()
        {
            var l = new PtrLenList();
            l.TryInclude(1, 3);
            Assert.IsFalse(l.TryExclude(0, 2));
            Assert.AreEqual("2-4", Str(l));
        }

        [Test]
        public void ExcludeFromEnd()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 4);
            Assert.IsTrue(l.TryExclude(2, 2));
            Assert.AreEqual("0-2", Str(l));
        }

        [Test]
        public void ExcludeFromEndOverlap()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 3);
            Assert.IsFalse(l.TryExclude(2, 2));
            Assert.AreEqual("0-2", Str(l));
        }

        [Test]
        public void ExcludeSplitRange()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 10);
            Assert.IsTrue(l.TryExclude(3, 2));
            Assert.AreEqual("0-3;5-10", Str(l));
        }

        [Test]
        public void SpanExclude()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 10; i++)
            {
                Assert.IsTrue(l.TryInclude(i * 4, 2));
            }
            Assert.IsFalse(l.TryExclude(5, 30));
            Assert.AreEqual("0-2;4-5;36-38", Str(l));
        }

        [Test]
        public void SpanExclude2()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 10; i++)
            {
                Assert.IsTrue(l.TryInclude(i * 4, 2));
            }
            Assert.IsFalse(l.TryExclude(24, 10));
            Assert.AreEqual("0-2;4-6;8-10;12-14;16-18;20-22;36-38", Str(l));
        }

        [Test]
        public void SpanExclude3()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 10; i++)
            {
                Assert.IsTrue(l.TryInclude(i * 4, 2));
            }
            Assert.IsFalse(l.TryExclude(24, 20));
            Assert.AreEqual("0-2;4-6;8-10;12-14;16-18;20-22", Str(l));
        }

        [Test]
        public void SpanExclude4()
        {
            var l = new PtrLenList();
            for (uint i = 0; i < 10; i++)
            {
                Assert.IsTrue(l.TryInclude(i * 4, 2));
            }
            Assert.IsFalse(l.TryExclude(16, 17));
            Assert.AreEqual("0-2;4-6;8-10;12-14;33-34;36-38", Str(l));
        }

        [Test]
        public void AfterClearIsEmpty()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            l.Clear();
            Assert.IsTrue(l.Empty);
        }

        [Test]
        public void CloneIsEqual()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            l.TryInclude(10, 11);
            var l2 = l.Clone();
            Assert.AreEqual("0-1;10-21", Str(l2));
            Assert.AreEqual("0-1;10-21", Str(l));
        }

        [Test]
        public void EmptyCloneIsEmpty()
        {
            var l = new PtrLenList();
            var l2 = l.Clone();
            Assert.IsTrue(l2.Empty);
        }

        [Test]
        public void CloneAndClearWorks()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 1);
            l.TryInclude(10, 11);
            var l2 = l.CloneAndClear();
            Assert.AreEqual("0-1;10-21", Str(l2));
            Assert.IsTrue(l.Empty);
        }

        [Test]
        public void EmptyCloneAndClearIsEmpty()
        {
            var l = new PtrLenList();
            var l2 = l.CloneAndClear();
            Assert.IsTrue(l2.Empty);
        }

        [Test]
        public void TryFindAndRemoveWholeRange()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(5, 5);
            l.TryInclude(15, 10);
            ulong pos;
            Assert.IsFalse(l.TryFindLenAndRemove(15, out pos));
            Assert.AreEqual("0-2;5-10;15-25", Str(l));
            Assert.IsTrue(l.TryFindLenAndRemove(5, out pos));
            Assert.AreEqual(5ul, pos);
            Assert.AreEqual("0-2;15-25", Str(l));
            Assert.IsTrue(l.TryFindLenAndRemove(10, out pos));
            Assert.AreEqual(15ul, pos);
            Assert.AreEqual("0-2", Str(l));
            Assert.IsTrue(l.TryFindLenAndRemove(2, out pos));
            Assert.AreEqual(0ul, pos);
            Assert.AreEqual("", Str(l));
        }

        [Test]
        public void TryFindAndRemovePartRange()
        {
            var l = new PtrLenList();
            l.TryInclude(0, 2);
            l.TryInclude(5, 5);
            l.TryInclude(15, 10);
            ulong pos;
            Assert.IsTrue(l.TryFindLenAndRemove(3, out pos));
            Assert.AreEqual(5ul, pos);
            Assert.AreEqual("0-2;8-10;15-25", Str(l));
            Assert.IsTrue(l.TryFindLenAndRemove(3, out pos));
            Assert.AreEqual(15ul, pos);
            Assert.AreEqual("0-2;8-10;18-25", Str(l));
            Assert.IsTrue(l.TryFindLenAndRemove(1, out pos));
            Assert.AreEqual(0ul, pos);
            Assert.AreEqual("1-2;8-10;18-25", Str(l));
        }

        [Test]
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
            Assert.AreEqual("0-2;5-6;9-10", Str(l1));
        }
    }
}
