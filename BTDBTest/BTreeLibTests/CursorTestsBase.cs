using BTDB.BTreeLib;
using System;
using System.Collections.Generic;
using Xunit;
using BTDB.Allocators;

namespace BTreeLibTest
{
    public abstract class CursorTestsBase : IDisposable
    {
        LeakDetectorWrapperAllocator _allocator;
        IRootNode _root;
        ICursor _cursor;

        public abstract bool Is12 { get; }
        public abstract ReadOnlySpan<byte> GetSampleValue(int index = 0);

        public CursorTestsBase()
        {
            _allocator = new LeakDetectorWrapperAllocator(new HGlobalAllocator());
            _root = Is12 ? BTreeImpl12.CreateEmptyRoot(_allocator) : null /*ARTImplV.CreateEmptyRoot(_allocator)*/;
            _cursor = _root.CreateCursor();
        }

        public void Dispose()
        {
            _root.Dispose();
            var leaks = _allocator.QueryAllocations();
            Assert.Equal(0ul, leaks.Count);
        }

        public static IEnumerable<object[]> InterestingValues()
        {
            for (int i = 0; i < 12; i++)
            {
                for (int j = i + 1; j < 12; j++)
                {
                    yield return new object[] {i, j};
                }
            }
        }

        public void InvalidCursorBehaviour()
        {
            Assert.Equal(-1, _cursor.CalcIndex());
        }

        public static IEnumerable<object[]> SampleKeys
        {
            get
            {
                var longKey = new byte[100000];
                new Random(1).NextBytes(longKey);
                var longKey2 = new byte[0x18fff];
                new Random(1).NextBytes(longKey2);
                return new List<object[]>
                {
                    new object[] {new byte[] {1, 2, 3}},
                    new object[] {new byte[] {1}},
                    new object[] {new byte[] { }},
                    new object[] {longKey},
                    new object[] {longKey2}
                };
            }
        }

        [Theory]
        [MemberData(nameof(SampleKeys))]
        public void CanInsertFirstData(byte[] key)
        {
            Assert.Equal(0, _root.GetCount());
            var val = GetSampleValue();
            Assert.True(_cursor.Upsert(key, val));
            Assert.Equal(1, _root.GetCount());
            Assert.Equal(key.Length, _cursor.GetKeyLength());
            Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val.ToArray(), _cursor.GetValue().ToArray());
        }

        [Theory]
        [MemberData(nameof(InterestingValues))]
        public void CanChangeValues(int valueIndex1, int valueIndex2)
        {
            var val = GetSampleValue(valueIndex1).ToArray();
            var val2 = GetSampleValue(valueIndex2).ToArray();
            _cursor.Upsert(new byte[] {1}, val);
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val, _cursor.GetValue().ToArray());
            _cursor.WriteValue(val2);
            Assert.Equal(val2.Length, _cursor.GetValueLength());
            Assert.Equal(val2, _cursor.GetValue().ToArray());
            _cursor.WriteValue(val);
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val, _cursor.GetValue().ToArray());
            using (var snapshot = _root.Snapshot())
            {
                _cursor.WriteValue(val2);
                Assert.Equal(val2.Length, _cursor.GetValueLength());
                Assert.Equal(val2, _cursor.GetValue().ToArray());
            }

            _cursor.Upsert(new byte[] {2}, val);
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val, _cursor.GetValue().ToArray());
            _cursor.WriteValue(val2);
            Assert.Equal(val2.Length, _cursor.GetValueLength());
            Assert.Equal(val2, _cursor.GetValue().ToArray());
            _cursor.WriteValue(val);
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val, _cursor.GetValue().ToArray());
            using (var snapshot = _root.Snapshot())
            {
                _cursor.WriteValue(val2);
                Assert.Equal(val2.Length, _cursor.GetValueLength());
                Assert.Equal(val2, _cursor.GetValue().ToArray());
            }
        }

        public static IEnumerable<object[]> SampleKeys2 =>
            new List<object[]>
            {
                new object[] {new byte[] {1, 2, 3}, new byte[] { }},
                new object[] {new byte[] {1, 2, 3}, new byte[] {1}},
                new object[] {new byte[] {1, 2, 3}, new byte[] {1, 2}},
                new object[] {new byte[] {1, 2, 3}, new byte[] {1, 2, 2}},
                new object[] {new byte[] {1, 2, 3}, new byte[] {1, 2, 4}},
                new object[] {new byte[] {1, 2, 3}, new byte[] {1, 1}},
                new object[] {new byte[] {1, 2, 3}, new byte[] {1, 3}},
            };

        [Theory]
        [MemberData(nameof(SampleKeys2))]
        public void CanInsertSecondKey(byte[] key, byte[] key2)
        {
            var val = GetSampleValue().ToArray();
            var val2 = GetSampleValue(3).ToArray();
            Assert.True(_cursor.Upsert(key, val));
            Assert.True(_cursor.Upsert(key2, val2));
            Assert.Equal(key2.Length, _cursor.GetKeyLength());
            Assert.Equal(key2, _cursor.FillByKey(new byte[key2.Length]).ToArray());
            Assert.Equal(val2.Length, _cursor.GetValueLength());
            Assert.Equal(val2, _cursor.GetValue().ToArray());
            Assert.Equal(2, _root.GetCount());
        }

        [Theory]
        [MemberData(nameof(SampleKeys))]
        public void SecondUpsertWithSameKeyJustOverwriteValue(byte[] key)
        {
            var val = GetSampleValue().ToArray();
            var val2 = GetSampleValue(3).ToArray();
            Assert.True(_cursor.Upsert(key, val));
            Assert.False(_cursor.Upsert(key, val2));
            Assert.Equal(key.Length, _cursor.GetKeyLength());
            Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
            Assert.Equal(val2.Length, _cursor.GetValueLength());
            Assert.Equal(val2, _cursor.GetValue().ToArray());
            Assert.Equal(1, _root.GetCount());
        }

        [Fact]
        public void MultipleInsertsInSingleTransaction()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[1];
            for (var i = 0; i < 256; i++)
            {
                key[0] = (byte) i;
                Assert.True(_cursor.Upsert(key, val));
                Assert.Equal(i, _cursor.CalcIndex());
                Assert.Equal(i + 1, _root.GetCount());
                Assert.Equal(key.Length, _cursor.GetKeyLength());
                Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
                Assert.Equal(val.Length, _cursor.GetValueLength());
                Assert.Equal(val, _cursor.GetValue().ToArray());
                for (var j = 0; j < 256; j++)
                {
                    key[0] = (byte) j;
                    Assert.Equal(j <= i, _cursor.FindExact(key));
                }
            }

            key = new byte[2];
            key[0] = 20;
            for (var j = 0; j < 256; j++)
            {
                key[1] = (byte) j;
                Assert.False(_cursor.FindExact(key));
            }

            for (var i = 0; i < 256; i++)
            {
                key[1] = (byte) i;
                Assert.True(_cursor.Upsert(key, val));
                Assert.Equal(256 + i + 1, _root.GetCount());
                Assert.Equal(20 + 1 + i, _cursor.CalcIndex());
                Assert.Equal(key.Length, _cursor.GetKeyLength());
                Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
                Assert.Equal(val.Length, _cursor.GetValueLength());
                Assert.Equal(val, _cursor.GetValue().ToArray());
                for (var j = 0; j < 256; j++)
                {
                    key[1] = (byte) j;
                    Assert.Equal(j <= i, _cursor.FindExact(key));
                }
            }

            Assert.False(_cursor.Upsert(key, val));
            Assert.Equal(key.Length, _cursor.GetKeyLength());
            Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val, _cursor.GetValue().ToArray());
            val = GetSampleValue(1).ToArray();
            Assert.False(_cursor.Upsert(key, val));
            Assert.Equal(key.Length, _cursor.GetKeyLength());
            Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val, _cursor.GetValue().ToArray());
            val = GetSampleValue(0).ToArray();
            Assert.False(_cursor.Upsert(key, val));
            Assert.Equal(key.Length, _cursor.GetKeyLength());
            Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
            Assert.Equal(val.Length, _cursor.GetValueLength());
            Assert.Equal(val, _cursor.GetValue().ToArray());
        }

        [Fact]
        public void MultipleInsertsInMultipleTransactions()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[1];
            for (var i = 0; i < 256; i++)
            {
                key[0] = (byte) i;
                var snapshot = _root.Snapshot();
                Assert.True(_cursor.Upsert(key, val));
                Assert.Equal(i + 1, _root.GetCount());
                Assert.Equal(key.Length, _cursor.GetKeyLength());
                Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
                Assert.Equal(val.Length, _cursor.GetValueLength());
                Assert.Equal(val, _cursor.GetValue().ToArray());
                for (var j = 0; j < 256; j++)
                {
                    key[0] = (byte) j;
                    Assert.Equal(j <= i, _cursor.FindExact(key));
                }

                var snapshotCursor = snapshot.CreateCursor();
                for (var j = 0; j < 256; j++)
                {
                    key[0] = (byte) j;
                    Assert.Equal(j < i, snapshotCursor.FindExact(key));
                }

                snapshot.Dispose();
            }

            key = new byte[2];
            key[0] = 20;
            for (var j = 0; j < 256; j++)
            {
                key[1] = (byte) j;
                Assert.False(_cursor.FindExact(key));
            }

            for (var i = 0; i < 256; i++)
            {
                key[1] = (byte) i;
                var snapshot = _root.Snapshot();
                Assert.True(_cursor.Upsert(key, val));
                Assert.Equal(256 + i + 1, _root.GetCount());
                Assert.Equal(key.Length, _cursor.GetKeyLength());
                Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
                Assert.Equal(val.Length, _cursor.GetValueLength());
                Assert.Equal(val, _cursor.GetValue().ToArray());
                for (var j = 0; j < 256; j++)
                {
                    key[1] = (byte) j;
                    Assert.Equal(j <= i, _cursor.FindExact(key));
                }

                var snapshotCursor = snapshot.CreateCursor();
                for (var j = 0; j < 256; j++)
                {
                    key[1] = (byte) j;
                    Assert.Equal(j < i, snapshotCursor.FindExact(key));
                }

                snapshot.Dispose();
            }
        }

        [Fact]
        public void ALotOfInsertsInIncreasingOrderWorks()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[4];
            for (var i = 0; i < 100000; i++)
            {
                BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                Assert.True(_cursor.Upsert(key, val));
                Assert.Equal(i + 1, _root.GetCount());
                Assert.Equal(key.Length, _cursor.GetKeyLength());
                Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
            }

            for (var i = 0; i < 100000; i++)
            {
                BTDB.Buffer.PackUnpack.PackInt32BE(key, 0, i);
                Assert.True(_cursor.FindExact(key));
                Assert.Equal(i, _cursor.CalcIndex());
            }
        }

        [Fact]
        public void SomeInsertsInIncreasingOrderWithSnapshotWorks()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            for (var i = 0; i < 200; i++)
            {
                key[0] = (byte) i;
                Assert.True(_cursor.Upsert(key, val));
                Assert.Equal(i + 1, _root.GetCount());
                Assert.Equal(key.Length, _cursor.GetKeyLength());
                Assert.Equal(key, _cursor.FillByKey(new byte[key.Length]).ToArray());
            }

            var startRoot = _root.Snapshot();
            key[0] = 0;
            key[1] = 1;
            Assert.True(_cursor.Upsert(key, val));
            startRoot.Dispose();
        }

        [Fact]
        public void FindFirstWorks()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[10];
            var keyBuffer = new byte[20];
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 0)));
            _cursor.Upsert(key, val);
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 0)));
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 1)));
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 9)));
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 10)));
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
            key[5] = 1;
            Assert.False(_cursor.FindFirst(key));
            key[5] = 0;
            for (int i = 0; i < 4; i++)
            {
                key[6] = (byte) i;
                _cursor.Upsert(key.AsSpan(0, 7), val);
            }

            Assert.False(_cursor.FindFirst(key.AsSpan(0, 9)));
            key[6] = 4;
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 9)));
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 7)));
            key[6] = 2;
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 8)));
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 7)));
            Assert.Equal(key.AsSpan(0, 7).ToArray(), _cursor.FillByKey(keyBuffer).ToArray());
            key[6] = 0;
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 6)));
            Assert.Equal(key.AsSpan(0, 7).ToArray(), _cursor.FillByKey(keyBuffer).ToArray());
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 5)));
            Assert.Equal(key.AsSpan(0, 7).ToArray(), _cursor.FillByKey(keyBuffer).ToArray());
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 2)));
            Assert.Equal(key.AsSpan(0, 7).ToArray(), _cursor.FillByKey(keyBuffer).ToArray());
            for (int i = 4; i < 16; i++)
            {
                key[6] = (byte) i;
                _cursor.Upsert(key.AsSpan(0, 7), val);
            }

            Assert.False(_cursor.FindFirst(key.AsSpan(0, 9)));
            key[6] = 16;
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 9)));
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 7)));
            key[6] = 8;
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 8)));
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 7)));
            Assert.Equal(key.AsSpan(0, 7).ToArray(), _cursor.FillByKey(keyBuffer).ToArray());
            key[6] = 0;
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 6)));
            Assert.Equal(key.AsSpan(0, 7).ToArray(), _cursor.FillByKey(keyBuffer).ToArray());
            for (int i = 32; i < 32 + 16; i++)
            {
                key[6] = (byte) i;
                _cursor.Upsert(key.AsSpan(0, 7), val);
            }

            Assert.False(_cursor.FindFirst(key.AsSpan(0, 9)));
            key[6] = 240;
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 9)));
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 7)));
            key[6] = 42;
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 8)));
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 7)));
            Assert.Equal(key.AsSpan(0, 7).ToArray(), _cursor.FillByKey(keyBuffer).ToArray());
            key[6] = 0;
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 6)));
            Assert.Equal(key.AsSpan(0, 7).ToArray(), _cursor.FillByKey(keyBuffer).ToArray());
            for (int i = 64; i < 255; i++)
            {
                key[6] = (byte) i;
                _cursor.Upsert(key.AsSpan(0, 7), val);
            }

            Assert.False(_cursor.FindFirst(key.AsSpan(0, 9)));
            key[6] = 24;
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 9)));
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 7)));
            key[6] = 100;
            Assert.False(_cursor.FindFirst(key.AsSpan(0, 8)));
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 7)));
            Assert.Equal(key.AsSpan(0, 7).ToArray(), _cursor.FillByKey(keyBuffer).ToArray());
            key[6] = 0;
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 6)));
            Assert.Equal(key.AsSpan(0, 7).ToArray(), _cursor.FillByKey(keyBuffer).ToArray());
            key[1] = 1;
            key[9] = 255;
            _cursor.Upsert(key, val);
            key[5] = 5;
            _cursor.Upsert(key, val);
            key[5] = 0;
            Assert.True(_cursor.FindFirst(key.AsSpan(0, 2)));
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
            for (int i = 254; i >= 253; i--)
            {
                key[9] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.True(_cursor.FindFirst(key.AsSpan(0, 7)));
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
            for (int i = 252; i >= 230; i--)
            {
                key[9] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.True(_cursor.FindFirst(key.AsSpan(0, 7)));
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
            for (int i = 229; i >= 1; i--)
            {
                key[9] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.True(_cursor.FindFirst(key.AsSpan(0, 7)));
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
        }

        [Fact]
        public void FindLastWorks()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[10];
            Assert.Equal(-1, _cursor.FindLastWithPrefix(key.AsSpan(0, 0)));
            for (var i = 0; i < 8000; i++)
            {
                key[2] = (byte) (i / 400);
                key[3] = (byte) (i / 20 % 20);
                key[4] = (byte) (i % 20);
                _cursor.Upsert(key.AsSpan(0, 7), val);
            }

            for (var i = 0; i < 20; i++)
            {
                key[2] = (byte) i;
                Assert.Equal(i * 400 + 399, _cursor.FindLastWithPrefix(key.AsSpan(0, 3)));
            }
        }

        [Fact]
        public void FindLastPrefixBugFixed()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[10];
            key[0] = 2;
            for (int i = 0; i < 4; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            key[1] = 1;
            Assert.Equal(1, _cursor.FindLastWithPrefix(key.AsSpan(0, 2)));
        }

        [Fact]
        public void FindExactSpecialCases()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            for (int i = 0; i < 3; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.False(_cursor.FindExact(key.AsSpan(0, 1)));
        }

        [Fact]
        public void MoveNextWorks()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            var keyBuffer = new byte[4];
            Assert.False(_cursor.MoveNext());
            _cursor.Upsert(key, val);
            Assert.False(_cursor.MoveNext());
            Assert.True(_cursor.MoveNext());
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
            for (int i = 1; i < 3; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.False(_cursor.MoveNext());
            for (int i = 0; i < 3; i++)
            {
                Assert.True(_cursor.MoveNext());
                Assert.Equal(i, _cursor.CalcIndex());
            }

            for (int i = 3; i < 15; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.False(_cursor.MoveNext());
            for (int i = 0; i < 15; i++)
            {
                Assert.True(_cursor.MoveNext());
                Assert.Equal(i, _cursor.CalcIndex());
            }

            for (int i = 16; i < 41; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.False(_cursor.MoveNext());
            for (int i = 0; i < 40; i++)
            {
                Assert.True(_cursor.MoveNext());
                Assert.Equal(i, _cursor.CalcIndex());
            }

            for (int i = 45; i < 80; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.False(_cursor.MoveNext());
            for (int i = 0; i < 74; i++)
            {
                Assert.True(_cursor.MoveNext());
                Assert.Equal(i, _cursor.CalcIndex());
            }
        }

        [Fact]
        public void MoveNextWorksWithIsLeaf()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            var keyBuffer = new byte[4];
            Assert.False(_cursor.MoveNext());
            _cursor.Upsert(key, val);
            _cursor.Upsert(key.AsSpan(0, 1), val);
            Assert.True(_cursor.MoveNext());
            Assert.False(_cursor.MoveNext());
            Assert.True(_cursor.MoveNext());
            Assert.True(_cursor.MoveNext());
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
            for (int i = 1; i < 3; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.False(_cursor.MoveNext());
            for (int i = 0; i < 4; i++)
            {
                Assert.True(_cursor.MoveNext());
                Assert.Equal(i, _cursor.CalcIndex());
            }

            for (int i = 3; i < 15; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.False(_cursor.MoveNext());
            for (int i = 0; i < 16; i++)
            {
                Assert.True(_cursor.MoveNext());
                Assert.Equal(i, _cursor.CalcIndex());
            }

            for (int i = 16; i < 41; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.False(_cursor.MoveNext());
            for (int i = 0; i < 41; i++)
            {
                Assert.True(_cursor.MoveNext());
                Assert.Equal(i, _cursor.CalcIndex());
            }

            for (int i = 45; i < 80; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            Assert.False(_cursor.MoveNext());
            for (int i = 0; i < 75; i++)
            {
                Assert.True(_cursor.MoveNext());
                Assert.Equal(i, _cursor.CalcIndex());
            }
        }

        [Fact]
        public void MovePreviousWorks()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            var keyBuffer = new byte[4];
            Assert.False(_cursor.MovePrevious());
            _cursor.Upsert(key, val);
            Assert.False(_cursor.MovePrevious());
            Assert.True(_cursor.MovePrevious());
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
            for (int i = 1; i < 3; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            for (int i = 0; i < 2; i++)
            {
                Assert.True(_cursor.MovePrevious());
                Assert.Equal(1 - i, _cursor.CalcIndex());
            }

            Assert.False(_cursor.MovePrevious());
            for (int i = 3; i < 15; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            for (int i = 0; i < 14; i++)
            {
                Assert.True(_cursor.MovePrevious());
                Assert.Equal(13 - i, _cursor.CalcIndex());
            }

            Assert.False(_cursor.MovePrevious());
            for (int i = 16; i < 41; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            for (int i = 0; i < 39; i++)
            {
                Assert.True(_cursor.MovePrevious());
                Assert.Equal(38 - i, _cursor.CalcIndex());
            }

            Assert.False(_cursor.MovePrevious());
            for (int i = 45; i < 80; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            for (int i = 0; i < 74; i++)
            {
                Assert.True(_cursor.MovePrevious());
                Assert.Equal(73 - i, _cursor.CalcIndex());
            }

            Assert.False(_cursor.MovePrevious());
        }

        [Fact]
        public void MovePreviousWorksWithIsLeaf()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            var keyBuffer = new byte[4];
            Assert.False(_cursor.MovePrevious());
            _cursor.Upsert(key, val);
            _cursor.Upsert(key.AsSpan(0, 1), val);
            Assert.False(_cursor.MovePrevious());
            Assert.True(_cursor.MovePrevious());
            Assert.Equal(key, _cursor.FillByKey(keyBuffer).ToArray());
            for (int i = 1; i < 3; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            for (int i = 0; i < 3; i++)
            {
                Assert.True(_cursor.MovePrevious());
                Assert.Equal(2 - i, _cursor.CalcIndex());
            }

            Assert.False(_cursor.MovePrevious());
            for (int i = 3; i < 15; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            for (int i = 0; i < 15; i++)
            {
                Assert.True(_cursor.MovePrevious());
                Assert.Equal(14 - i, _cursor.CalcIndex());
            }

            Assert.False(_cursor.MovePrevious());
            for (int i = 16; i < 41; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            for (int i = 0; i < 40; i++)
            {
                Assert.True(_cursor.MovePrevious());
                Assert.Equal(39 - i, _cursor.CalcIndex());
            }

            Assert.False(_cursor.MovePrevious());
            for (int i = 45; i < 80; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            for (int i = 0; i < 75; i++)
            {
                Assert.True(_cursor.MovePrevious());
                Assert.Equal(74 - i, _cursor.CalcIndex());
            }

            Assert.False(_cursor.MovePrevious());
        }

        [Fact]
        public void SeekIndexWorks()
        {
            Assert.False(_cursor.SeekIndex(-1));
            Assert.False(_cursor.SeekIndex(0));
            Assert.False(_cursor.SeekIndex(1));
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            var total = 0;
            _cursor.Upsert(key, val);
            total++;
            Assert.Equal(total, _root.GetCount());
            CheckSeekIndex();
            for (int i = 1; i < 3; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
                total++;
                Assert.Equal(total, _root.GetCount());
            }

            CheckSeekIndex();
            for (int i = 4; i < 15; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
                total++;
                Assert.Equal(total, _root.GetCount());
            }

            CheckSeekIndex();
            for (int i = 16; i < 40; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
                total++;
                Assert.Equal(total, _root.GetCount());
            }

            CheckSeekIndex();
            for (int i = 41; i < 240; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
                total++;
                Assert.Equal(total, _root.GetCount());
            }

            CheckSeekIndex();
            _cursor.Upsert(key.AsSpan(0, 1), val);
            total++;
            Assert.Equal(total, _root.GetCount());
            CheckSeekIndex();
        }

        void CheckSeekIndex()
        {
            for (int i = 0; i < _root.GetCount(); i++)
            {
                Assert.True(_cursor.SeekIndex(i));
                Assert.Equal(i, _cursor.CalcIndex());
            }

            Assert.False(_cursor.SeekIndex(_root.GetCount()));
        }

        [Fact]
        public void SingleOnlyKeyEraseWorks()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            _cursor.Upsert(key, val);
            Assert.Equal(1, _cursor.EraseTo(_cursor));
            Assert.False(_cursor.IsValid());
            Assert.Equal(0, _root.GetCount());
        }

        [Fact]
        public void EraseMiddle4Works()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            for (int i = 0; i < 2; i++)
            {
                key[0] = (byte) i;
                _cursor.Upsert(key.AsSpan(0, 1), val);
            }

            for (int i = 0; i < 4; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            key[1] = 1;
            _cursor.FindExact(key);
            var c2 = _cursor.Clone();
            c2.MoveNext();
            Assert.Equal(2, _cursor.EraseTo(c2));
            Assert.Equal(4, _root.GetCount());
        }

        [Fact]
        public void EraseMiddle16Works()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            for (int i = 0; i < 2; i++)
            {
                key[0] = (byte) i;
                _cursor.Upsert(key.AsSpan(0, 1), val);
            }

            for (int i = 0; i < 16; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            key[1] = 5;
            _cursor.FindExact(key);
            var c2 = _cursor.Clone();
            c2.MoveNext();
            c2.MoveNext();
            Assert.Equal(3, _cursor.EraseTo(c2));
            Assert.Equal(15, _root.GetCount());
        }

        [Fact]
        public void EraseMiddle48Works()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            for (int i = 0; i < 2; i++)
            {
                key[0] = (byte) i;
                _cursor.Upsert(key.AsSpan(0, 1), val);
            }

            for (int i = 0; i < 48; i++)
            {
                key[1] = (byte) (i * 2);
                _cursor.Upsert(key, val);
            }

            key[1] = 8;
            _cursor.FindExact(key);
            var c2 = _cursor.Clone();
            c2.MoveNext();
            c2.MoveNext();
            Assert.Equal(3, _cursor.EraseTo(c2));
            Assert.Equal(47, _root.GetCount());
        }

        [Fact]
        public void EraseMiddle256Works()
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            for (int i = 0; i < 2; i++)
            {
                key[0] = (byte) i;
                _cursor.Upsert(key.AsSpan(0, 1), val);
            }

            for (int i = 0; i < 120; i++)
            {
                key[1] = (byte) (i * 2);
                _cursor.Upsert(key, val);
            }

            key[1] = 10;
            _cursor.FindExact(key);
            var c2 = _cursor.Clone();
            c2.MoveNext();
            c2.MoveNext();
            Assert.Equal(3, _cursor.EraseTo(c2));
            Assert.Equal(119, _root.GetCount());
        }

        [Theory]
        [InlineData(4)]
        [InlineData(16)]
        [InlineData(48)]
        [InlineData(100)]
        public void EraseToOneChild(int count)
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[3];
            for (int i = 0; i < count; i++)
            {
                key[1] = (byte) (i * 2);
                key[2] = (byte) (i + 5);
                _cursor.Upsert(key, val);
            }

            key[1] = 2;
            key[2] = 6;
            _cursor.FindExact(key);
            var c2 = _cursor.Clone();
            c2.SeekIndex(c2.FindLastWithPrefix(new ReadOnlySpan<byte>()));
            Assert.Equal(count - 1, _cursor.EraseTo(c2));
            Assert.Equal(1, _root.GetCount());
            _cursor.FindFirst(new ReadOnlySpan<byte>());
            _cursor.FillByKey(key);
            Assert.Equal(0, key[0]);
            Assert.Equal(0, key[1]);
            Assert.Equal(5, key[2]);
            Assert.Equal(val, _cursor.GetValue().ToArray());
            _cursor.Erase();
            for (int i = 0; i < count; i++)
            {
                key[1] = (byte) (i * 2);
                _cursor.Upsert(key, val);
            }

            key[1] = 0;
            _cursor.FindExact(key);
            c2 = _cursor.Clone();
            c2.SeekIndex(c2.FindLastWithPrefix(new ReadOnlySpan<byte>()));
            c2.MovePrevious();
            Assert.Equal(count - 1, _cursor.EraseTo(c2));
            Assert.Equal(1, _root.GetCount());
            _cursor.FindFirst(new ReadOnlySpan<byte>());
            _cursor.FillByKey(key);
            Assert.Equal(0, key[0]);
            Assert.Equal((count - 1) * 2, key[1]);
            Assert.Equal(val, _cursor.GetValue().ToArray());
        }

        [Theory]
        [InlineData(0, 3, 1, 2)]
        [InlineData(0, 15, 1, 2)]
        [InlineData(0, 15, 1, 14)]
        [InlineData(0, 47, 3, 4)]
        [InlineData(0, 46, 2, 45)]
        [InlineData(0, 46, 1, 35)]
        [InlineData(0, 255, 5, 6)]
        [InlineData(0, 254, 10, 250)]
        public void EraseToWithSnapshot(int datafrom, int datato, int erasefrom, int eraseto)
        {
            var val = GetSampleValue().ToArray();
            var key = new byte[2];
            for (int i = datafrom; i <= datato; i++)
            {
                key[1] = (byte) i;
                _cursor.Upsert(key, val);
            }

            key[1] = (byte) erasefrom;
            _cursor.FindExact(key);
            var c2 = _cursor.Clone();
            key[1] = (byte) eraseto;
            c2.FindExact(key);
            var snapshot = _root.Snapshot();
            Assert.Equal(eraseto - erasefrom + 1, _cursor.EraseTo(c2));
            Assert.Equal(datato - datafrom - (eraseto - erasefrom), _root.GetCount());
            Assert.Equal(datato - datafrom + 1, snapshot.GetCount());
            snapshot.Dispose();
        }

        [Theory]
        [InlineData(20, 128, 200, 20, 46153)]
        [InlineData(24, 20, 24, 24, 5)]
        [InlineData(0, 128, 255, 20, 65428)]
        [InlineData(0, 10, 255, 255, 65781)]
        [InlineData(0, 0, 255, 245, 65781)]
        [InlineData(10, 128, 245, 128, 60396)]
        [InlineData(10, 128, 10, 128, 1)]
        [InlineData(40, 0, 40, 255, 256)]
        [InlineData(40, -1, 40, 255, 257)]
        public void BigErase256Works(byte l0, int l1, byte r0, byte r1, int erased)
        {
            var key = new byte[2];
            for (int i = 0; i <= 255; i++)
            {
                key[0] = (byte) i;
                for (int j = 0; j <= 255; j++)
                {
                    key[1] = (byte) j;
                    _cursor.Upsert(key, GetSampleValue(j));
                }

                _cursor.Upsert(key.AsSpan(0, 1), GetSampleValue(i));
            }

            key[0] = l0;
            key[1] = (byte) l1;
            _cursor.FindExact(key.AsSpan(0, 1 + (l1 >= 0 ? 1 : 0)));
            var c2 = _cursor.Clone();
            key[0] = r0;
            key[1] = r1;
            c2.FindExact(key);
            var snapshot = _root.Snapshot();
            Assert.Equal(erased, _cursor.EraseTo(c2));
            Assert.Equal(65792 - erased, _root.GetCount());
            Assert.Equal(65792, snapshot.GetCount());
            snapshot.Dispose();
            for (int i = 0; i <= 255; i++)
            {
                key[0] = (byte) i;
                for (int j = 0; j <= 255; j++)
                {
                    key[1] = (byte) j;
                    if (_cursor.FindExact(key))
                        Assert.Equal(GetSampleValue(j).ToArray(), _cursor.GetValue().ToArray());
                }

                if (_cursor.FindExact(key.AsSpan(0, 1)))
                    Assert.Equal(GetSampleValue(i).ToArray(), _cursor.GetValue().ToArray());
            }
        }

        [Theory]
        [InlineData(20, 22, 22, 20, 63)]
        [InlineData(20, 22, 22, 40, 83)]
        [InlineData(20, -1, 22, 40, 96)]
        public void BigErase48Works(byte l0, int l1, byte r0, byte r1, int erased)
        {
            var key = new byte[2];
            for (int i = 10; i <= 40; i++)
            {
                key[0] = (byte) i;
                for (int j = 10; j <= 40; j++)
                {
                    key[1] = (byte) j;
                    _cursor.Upsert(key, GetSampleValue(j));
                }

                _cursor.Upsert(key.AsSpan(0, 1), GetSampleValue(i));
            }

            key[0] = l0;
            key[1] = (byte) l1;
            _cursor.FindExact(key.AsSpan(0, 1 + (l1 >= 0 ? 1 : 0)));
            var c2 = _cursor.Clone();
            key[0] = r0;
            key[1] = r1;
            c2.FindExact(key);
            var snapshot = _root.Snapshot();
            Assert.Equal(erased, _cursor.EraseTo(c2));
            Assert.Equal(992 - erased, _root.GetCount());
            Assert.Equal(992, snapshot.GetCount());
            snapshot.Dispose();
            for (int i = 10; i <= 40; i++)
            {
                key[0] = (byte) i;
                for (int j = 10; j <= 40; j++)
                {
                    key[1] = (byte) j;
                    if (_cursor.FindExact(key))
                        Assert.Equal(GetSampleValue(j).ToArray(), _cursor.GetValue().ToArray());
                }

                if (_cursor.FindExact(key.AsSpan(0, 1)))
                    Assert.Equal(GetSampleValue(i).ToArray(), _cursor.GetValue().ToArray());
            }
        }
    }
}
