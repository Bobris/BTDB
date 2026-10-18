using System;
using System.Collections.Generic;
using BTDB;
using BTDB.EventStoreLayer;
using Xunit;
using static BTDBTest.EventLayerRoundTripHelper;

namespace BTDBTest;

public class ObjectTypeDescriptorCoverageTest
{
    [Generate]
    public class ExplicitAccessors
    {
        string? _name;
        int _number;
        decimal? _amount;
        Guid? _id;
        (int, object?) _pair;

        public string? Name { get => Same(_name); set => _name = value; }
        public int Number { get => Same(_number); set => _number = value; }
        public decimal? Amount { get => Same(_amount); set => _amount = value; }
        public Guid? Id { get => Same(_id); set => _id = value; }
        public List<int>? Items { get; set; }
        public (int, object?) Pair { get => Same(_pair); set => _pair = value; }

        static T Same<T>(T value) => value;
    }

    static ExplicitAccessors CreateExplicitAccessors() => new()
    {
        Name = "n", Number = 42, Amount = 12.5m, Id = Guid.Parse("01234567-89ab-cdef-0123-456789abcdef"),
        Items = [1, 2], Pair = (3, new ExplicitAccessorsNested { Text = "nested" })
    };

    static void AssertExplicitAccessors(object? result)
    {
        var r = Assert.IsType<ExplicitAccessors>(result);
        Assert.Equal("n", r.Name);
        Assert.Equal(42, r.Number);
        Assert.Equal(12.5m, r.Amount);
        Assert.Equal(Guid.Parse("01234567-89ab-cdef-0123-456789abcdef"), r.Id);
        Assert.Equal([1, 2], r.Items);
        Assert.Equal(3, r.Pair.Item1);
        Assert.Equal("nested", Assert.IsType<ExplicitAccessorsNested>(r.Pair.Item2).Text);
    }

    [Generate]
    public class ExplicitAccessorsNested
    {
        public string? Text { get; set; }
    }

    [Fact]
    public void PropertiesWithExplicitAccessorsRoundTripInEventStore2()
    {
        AssertExplicitAccessors(RoundTrip2(CreateExplicitAccessors()));
    }

    [Fact]
    public void PropertiesWithExplicitAccessorsRoundTripInEventStore1()
    {
        AssertExplicitAccessors(RoundTrip1(CreateExplicitAccessors()));
    }

    [Fact]
    public void PropertiesWithExplicitAccessorsRoundTripInTypeSerializers()
    {
        AssertExplicitAccessors(RoundTripMapping(CreateExplicitAccessors()));
    }
}
