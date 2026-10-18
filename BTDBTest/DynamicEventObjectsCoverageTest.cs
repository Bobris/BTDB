using System;
using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.EventStoreLayer;
using Xunit;
using static BTDBTest.EventLayerRoundTripHelper;

namespace BTDBTest;

public class DynamicEventObjectsCoverageTest
{
    [Flags]
    public enum Perm : ushort
    {
        None = 0,
        Read = 1,
        Write = 2,
        Exec = 4
    }

    [Flags]
    public enum NoZeroFlags
    {
        A = 1,
        B = 2
    }

    public enum Signed : long
    {
        Minus = -1,
        Plus = 1
    }

    [Generate]
    public class Sample
    {
        public string? Name { get; set; }
        public int Num { get; set; }
        public List<string>? Tags { get; set; }
        public Dictionary<string, int>? Map { get; set; }
        public Perm Perm { get; set; }
        public Perm PermNone { get; set; }
        public Perm PermUnknown { get; set; }
        public NoZeroFlags NoZero { get; set; }
        public Signed Signed { get; set; }
        public Signed SignedUnknown { get; set; }
        public bool Flag { get; set; }
        public Guid G { get; set; }
        public string? Quoted { get; set; }
    }

    static Sample CreateSample() => new()
    {
        Name = "n", Num = 7, Tags = ["a", "b"], Map = new() { ["k"] = 1 }, Perm = Perm.Read | Perm.Exec,
        PermNone = Perm.None, PermUnknown = (Perm)9, NoZero = 0, Signed = Signed.Minus, SignedUnknown = (Signed)(-5),
        Flag = true, G = Guid.Empty, Quoted = "a\"b\\c\r\n"
    };

    static RenamingTypeMapper HideAll() => new RenamingTypeMapper()
        .Hide(new FullNameTypeMapper().ToName(typeof(Sample)))
        .Hide(new FullNameTypeMapper().ToName(typeof(Perm)))
        .Hide(new FullNameTypeMapper().ToName(typeof(NoZeroFlags)))
        .Hide(new FullNameTypeMapper().ToName(typeof(Signed)));

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void DynamicObjectToStringIsJsonLike(EventLayer layer)
    {
        var result = RoundTrip(layer, CreateSample(), readMapper: HideAll());
        Assert.Equal(
            "{ \"Flag\": \"True\", \"G\": \"00000000-0000-0000-0000-000000000000\", \"Map\": { \"k\": 1 }, " +
            "\"Name\": \"n\", \"NoZero\": \"0\", \"Num\": 7, \"Perm\": \"Read, Exec\", \"PermNone\": \"None\", " +
            "\"PermUnknown\": \"9\", \"Quoted\": \"a\\\"b\\\\c\\r\\n\", \"Signed\": \"Minus\", " +
            "\"SignedUnknown\": \"-5\", \"Tags\": [ \"a\", \"b\" ] }",
            result!.ToString());
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void DynamicObjectSupportsDynamicMemberAccess(EventLayer layer)
    {
        dynamic d = RoundTrip(layer, CreateSample(), readMapper: HideAll())!;
        Assert.Equal("n", (string)d.Name);
        Assert.Equal(7, (int)d.Num);
        d.Num = 8;
        Assert.Equal(8, (int)d.Num);
        Assert.Throws<MemberAccessException>(() => (object)d.Missing);
        Assert.Throws<MemberAccessException>(() => d.Missing = 1);
        var provider = (System.Dynamic.IDynamicMetaObjectProvider)d;
        var meta = provider.GetMetaObject(System.Linq.Expressions.Expression.Constant(provider));
        Assert.Contains("Name", meta.GetDynamicMemberNames());
        Assert.Equal(13, meta.GetDynamicMemberNames().Count());
    }

    [Fact]
    public void DynamicObjectMemberAccessWorksAcrossDifferentDescriptors()
    {
        // Same call site used with objects of two different descriptors must use slow path by name
        var a = RoundTrip(EventLayer.Store2, CreateSample(), readMapper: HideAll())!;
        var b = RoundTrip(EventLayer.Mapping, CreateSample(), readMapper: HideAll())!;
        foreach (var o in new[] { a, b, a })
        {
            dynamic d = o;
            Assert.Equal("n", (string)d.Name);
            d.Name = "changed";
            Assert.Equal("changed", (string)d.Name);
            d.Name = "n";
            Assert.Throws<MemberAccessException>(() => (object)d.Unknown);
        }
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void DynamicEnumBehavesLikeEnum(EventLayer layer)
    {
        var fields = EventLayerScenarios.Fields(RoundTrip(layer, CreateSample(), readMapper: HideAll()));
        var perm = Assert.IsType<EnumTypeDescriptor.DynamicEnum>(fields["Perm"]);
        Assert.Equal("Read, Exec", perm.ToString());
        Assert.Equal(5UL.GetHashCode(), perm.GetHashCode());
        Assert.True(perm.Equals(perm));
        Assert.False(perm.Equals(null));
        Assert.False(perm.Equals("Read"));
        Assert.False(perm.Equals(fields["PermNone"]));
        Assert.False(perm.Equals(fields["Signed"]));
        Assert.NotNull(perm.GetDescriptor());
        Assert.Contains("enum flags {", perm.GetDescriptor().Describe());
        // Different enum type (with different descriptor) is never equal
        Assert.False(perm.Equals(Signed.Plus));
        var signed = Assert.IsType<EnumTypeDescriptor.DynamicEnum>(fields["Signed"]);
        Assert.Equal("Minus", signed.ToString());
        Assert.Contains("Minus = -1", signed.GetDescriptor().Describe());
    }

    [Fact]
    public void DynamicEnumEqualsRealEnumWithSameShape()
    {
        // Mapping layer merges descriptors by shape, so DynamicEnum is created only when type is hidden,
        // still its descriptor equals to descriptor of real enum type known by same TypeSerializers
        var ts = new TypeSerializers(HideAll());
        var descriptor = ts.DescriptorOf(typeof(Perm))!;
        var dynamicEnum = new EnumTypeDescriptor.DynamicEnum(5UL, descriptor);
        Assert.True(dynamicEnum.Equals(Perm.Read | Perm.Exec));
        Assert.False(dynamicEnum.Equals(Perm.Read));
        var signedDescriptor = ts.DescriptorOf(typeof(Signed))!;
        Assert.True(new EnumTypeDescriptor.DynamicEnum(-1L, signedDescriptor).Equals(Signed.Minus));
        Assert.False(new EnumTypeDescriptor.DynamicEnum(1L, signedDescriptor).Equals(Signed.Minus));
        Assert.False(new EnumTypeDescriptor.DynamicEnum(1L, signedDescriptor).Equals(dynamicEnum));
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void DynamicCollectionsKnowTheirDescriptorsAndPrintAsJson(EventLayer layer)
    {
        var fields = EventLayerScenarios.Fields(RoundTrip(layer, CreateSample(), readMapper: HideAll()));
        var tags = Assert.IsType<ListWithDescriptor<object?>>(fields["Tags"]);
        Assert.Equal("[ \"a\", \"b\" ]", tags.ToString());
        Assert.Equal("List<String>", tags.GetDescriptor().Name);
        var map = Assert.IsType<DictionaryWithDescriptor<object, object?>>(fields["Map"]);
        Assert.Equal("{ \"k\": 1 }", map.ToString());
        Assert.Equal("Dictionary<String, VInt32>", map.GetDescriptor().Name);
    }

    [Fact]
    public void CollectionsWithDescriptorPrintEmptyAndMultipleItems()
    {
        var ts = new TypeSerializers();
        var listDescriptor = ts.DescriptorOf(typeof(List<int>))!;
        var list = new ListWithDescriptor<int>(2, listDescriptor) { 1, 2 };
        Assert.Equal("[ 1, 2 ]", list.ToString());
        Assert.Equal("[  ]", new ListWithDescriptor<int>(0, listDescriptor).ToString());
        var set = new HashSetWithDescriptor<string>(2, listDescriptor) { "x", "y" };
        Assert.Equal("[ \"x\", \"y\" ]", set.ToString());
        Assert.Same(listDescriptor, set.GetDescriptor());
        var dictDescriptor = ts.DescriptorOf(typeof(Dictionary<int, bool>))!;
        var dict = new DictionaryWithDescriptor<int, bool>(2, dictDescriptor) { [1] = true, [2] = false };
        Assert.Equal("{ 1: \"True\", 2: \"False\" }", dict.ToString());
        var ordered = new OrderedDictionaryWithDescriptor<int, bool>(1, dictDescriptor) { [3] = true };
        Assert.Equal("{ 3: \"True\" }", ordered.ToString());
        Assert.Throws<NotSupportedException>(() => ordered.GetAdvancedEnumerator(default));
        Assert.Throws<NotSupportedException>(() => ordered.GetDecreasingEnumerator(1));
        Assert.Throws<NotSupportedException>(() => ordered.GetIncreasingEnumerator(1));
        Assert.Throws<NotSupportedException>(() => ordered.GetReverseEnumerator());
        Assert.Throws<NotSupportedException>(() => ordered.RemoveRange(1, true, 2, true));
    }
}
