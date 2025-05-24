using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests;

public class MemoryLayoutTest
{
    // This clones layout of Dictionary<TKey, TValue>.Entry
    struct DictionaryEntry<TKey, TValue> where TKey : notnull
    {
        public uint hashCode;
        public int next;
        public TKey key;
        public TValue value;
    }

    static (uint OffsetNext, uint OffsetKey, uint OffsetValue, uint Size) GetDictionaryEntryLayout<TKey, TValue>()
    {
        DictionaryEntry<TKey, TValue> entry = default;
        var offsetNext = (uint)Unsafe.ByteOffset(ref Unsafe.As<DictionaryEntry<TKey, TValue>, byte>(ref entry),
            ref Unsafe.As<int, byte>(ref entry.next));
        var offsetKey = (uint)Unsafe.ByteOffset(ref Unsafe.As<DictionaryEntry<TKey, TValue>, byte>(ref entry),
            ref Unsafe.As<TKey, byte>(ref entry.key));
        var offsetValue = (uint)Unsafe.ByteOffset(ref Unsafe.As<DictionaryEntry<TKey, TValue>, byte>(ref entry),
            ref Unsafe.As<TValue, byte>(ref entry.value));
        var size = (uint)Unsafe.SizeOf<DictionaryEntry<TKey, TValue>>();
        return (offsetNext, offsetKey, offsetValue, size);
    }

    void CheckGetDictionaryEntryLayout<TKey, TValue>()
    {
        var (offsetNext, offsetKey, offsetValue, size) = GetDictionaryEntryLayout<TKey, TValue>();
        var (calcNext, calcKey, calcValue, calcSize) = RawData.GetDictionaryEntriesLayout(typeof(TKey), typeof(TValue));
        Assert.Equal(offsetNext, calcNext);
        Assert.Equal(offsetKey, calcKey);
        Assert.Equal(offsetValue, calcValue);
        Assert.Equal(size, calcSize);
    }

    public struct RefAndInt
    {
        object Obj;
        int Int;
    }

    [Generate]
    public class RegisterSampleTypes
    {
        public IDictionary<int, int> T1;
        public IDictionary<string, int> T2;
        public IDictionary<int, string> T3;
        public IDictionary<string, string> T4;
        public Dictionary<int, long> T5;
        public IDictionary<long, int> T6;
        public IDictionary<long, long> T7;
        public Dictionary<string, long> T8;
        public IDictionary<long, string> T9;
        public IDictionary<byte, byte> T10;
        public IDictionary<byte, Int128> T11;
        public Dictionary<Int128, byte> T12;
        public IDictionary<Int128, Int128> T13;
        public Dictionary<Int128, string> T14;
        public IDictionary<string, Int128> T15;
        public IDictionary<string, byte> T16;
        public Dictionary<byte, string> T17;
        public IDictionary<byte, long> T18;
        public IDictionary<long, byte> T19;
        public Dictionary<byte, RefAndInt> T20;
        public IDictionary<RefAndInt, byte> T21;
    }

    [Fact]
    public void VerifyGetDictionaryEntriesLayout()
    {
        CheckGetDictionaryEntryLayout<int, int>();
        CheckGetDictionaryEntryLayout<string, int>();
        CheckGetDictionaryEntryLayout<int, string>();
        CheckGetDictionaryEntryLayout<string, string>();
        CheckGetDictionaryEntryLayout<int, long>();
        CheckGetDictionaryEntryLayout<long, int>();
        CheckGetDictionaryEntryLayout<long, long>();
        CheckGetDictionaryEntryLayout<string, long>();
        CheckGetDictionaryEntryLayout<long, string>();
        CheckGetDictionaryEntryLayout<byte, byte>();
        CheckGetDictionaryEntryLayout<byte, Int128>();
        CheckGetDictionaryEntryLayout<Int128, byte>();
        CheckGetDictionaryEntryLayout<Int128, Int128>();
        CheckGetDictionaryEntryLayout<Int128, string>();
        CheckGetDictionaryEntryLayout<string, Int128>();
        CheckGetDictionaryEntryLayout<string, byte>();
        CheckGetDictionaryEntryLayout<byte, string>();
        CheckGetDictionaryEntryLayout<byte, long>();
        CheckGetDictionaryEntryLayout<long, byte>();
        CheckGetDictionaryEntryLayout<byte, RefAndInt>();
        CheckGetDictionaryEntryLayout<RefAndInt, byte>();
    }
}
