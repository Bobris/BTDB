using System;
using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests;

public class RawDataCoverageTest
{
    [Fact]
    public void MethodTableFlagsMatchReflection()
    {
        Assert.True(RawData.MethodTableOf(typeof(int[])).HasComponentSize);
        Assert.True(RawData.MethodTableOf(typeof(string)).HasComponentSize);
        Assert.False(RawData.MethodTableOf(typeof(object)).HasComponentSize);
        Assert.True(RawData.MethodTableOf(typeof(object[])).ContainsGCPointers);
        Assert.False(RawData.MethodTableOf(typeof(int[])).ContainsGCPointers);
        Assert.True(RawData.MethodTableOf(typeof(int)).IsValueType);
        Assert.False(RawData.MethodTableOf(typeof(string)).IsValueType);
        Assert.True(RawData.MethodTableOf(typeof(int?)).IsNullable);
        Assert.False(RawData.MethodTableOf(typeof(int)).IsNullable);
        Assert.True(RawData.MethodTableOf(typeof(List<int>)).HasInstantiation);
        Assert.False(RawData.MethodTableOf(typeof(object)).HasInstantiation);
        Assert.True(RawData.MethodTableOf(typeof(List<int>)).IsConstructedGenericType);
        Assert.False(RawData.MethodTableOf(typeof(object)).IsConstructedGenericType);
        Assert.True(RawData.MethodTableOf(typeof(List<>)).IsGenericTypeDefinition);
        Assert.False(RawData.MethodTableOf(typeof(List<int>)).IsGenericTypeDefinition);
        Assert.True(RawData.MethodTableOf(typeof(int[,])).IsMultiDimensionalArray);
        Assert.Equal(2, RawData.MethodTableOf(typeof(int[,])).MultiDimensionalArrayRank);
        Assert.False(RawData.MethodTableOf(typeof(int[])).IsMultiDimensionalArray);
        Assert.True(RawData.MethodTableOf(typeof(List<int>)).InterfaceCount > 0);
        Assert.False(RawData.MethodTableOf(typeof(int)).HasTypeEquivalence);
        _ = RawData.MethodTableOf(typeof(object)).HasDefaultConstructor;
        _ = RawData.MethodTableOf(typeof(IDisposable)).NonTrivialInterfaceCast;
        unsafe
        {
            Assert.True(RawData.MethodTableOf(typeof(string)).ParentMethodTable ==
                        (RawData.MethodTable*)typeof(object).TypeHandle.Value);
        }
    }

    [Fact]
    public void SizeAlignHelpers()
    {
        Assert.Equal(16u, RawData.Align(9, 8));
        Assert.Equal(8u, RawData.Align(8, 8));
        Assert.Equal(8u, RawData.CombineAlign(4, 8));
        Assert.Equal(8u, RawData.CombineAlign(8, 4));
        Assert.True(RawData.IsRefOrContainsRef(typeof(string)));
        Assert.True(RawData.IsRefOrContainsRef(typeof(KeyValuePair<int, string>)));
        Assert.False(RawData.IsRefOrContainsRef(typeof(int)));
        Assert.Equal((8u, 4u), RawData.GetSizeAndAlign(typeof(int?)));
        Assert.Equal((1u, 1u), RawData.GetSizeAndAlign(typeof(DayOfWeekByte)));
        Assert.Equal((16u, 8u), RawData.GetSizeAndAlign(typeof(KeyValuePair<int, long>)));
    }

    public enum DayOfWeekByte : byte
    {
        Monday
    }

    [Fact]
    public void EntriesLayoutForReferenceTypesIsFixed()
    {
        Assert.Equal((12u, 0u, 16u), RawData.GetHashSetEntriesLayout(typeof(string)));
        Assert.Equal((20u, 0u, 8u, 24u), RawData.GetDictionaryEntriesLayout(typeof(string), typeof(object)));
    }

    public struct UnregisteredStruct
    {
        public decimal A;
        public Guid B;
    }

    [Fact]
    public void EntriesLayoutForUnregisteredValueTypesThrows()
    {
        Assert.Throws<BTDBException>(() => RawData.GetHashSetEntriesLayout(typeof(UnregisteredStruct)));
        Assert.Throws<BTDBException>(() =>
            RawData.GetDictionaryEntriesLayout(typeof(UnregisteredStruct), typeof(UnregisteredStruct)));
    }

    public struct StructWithReference
    {
        public string S;
    }

    public struct BigStruct
    {
        public long A, B, C;
    }

    public struct SmallStruct
    {
        public long A;
    }

    [Fact]
    public unsafe void StackAllocatorsForValueTypes()
    {
        Assert.Throws<InvalidOperationException>(() => FindAllocator(typeof(StructWithReference)));
        Assert.Throws<InvalidOperationException>(() => FindAllocator(typeof(BigStruct)));
        Assert.True(FindAllocator(typeof(SmallStruct)));
        Assert.True(FindAllocator(typeof(string)));
    }

    static unsafe bool FindAllocator(Type type) => ReflectionMetadata.FindStackAllocatorByType(type) != null;

    [Fact]
    public void ReflectionMetadataLookups()
    {
        var all = ReflectionMetadata.All();
        Assert.NotEmpty(all);
        var some = all.First(m => m.TruePersistedName.Length < 200);
        Assert.Same(some, ReflectionMetadata.FindByName(some.TruePersistedName.AsSpan()));
        Assert.Same(some, ReflectionMetadata.FindByType(some.Type));
        Assert.Null(ReflectionMetadata.FindByName("Surely.Not.Registered.Type".AsSpan()));
        // Before the fix names over 256 characters threw, so a registered type with a longer name was not found.
        Assert.Null(ReflectionMetadata.FindByName(new string('x', 257).AsSpan()));
        var longName = new ClassMetadata
        {
            Type = typeof(LongNamed), Name = "LongNamed", Namespace = "", PersistedName = new string('L', 300),
            Fields = [], Implements = []
        };
        ReflectionMetadata.Register(longName);
        Assert.Same(longName, ReflectionMetadata.FindByName(longName.TruePersistedName.AsSpan()));
        Assert.Null(ReflectionMetadata.FindCollectionByType(typeof(UnregisteredStruct)));
        Assert.Null(ReflectionMetadata.FindCollectionByElementType(typeof(UnregisteredStruct)));
        // registering the same metadata again is no-op
        ReflectionMetadata.Register(some);
        Assert.Same(some, ReflectionMetadata.FindByType(some.Type));
    }

    class LongNamed;
}
