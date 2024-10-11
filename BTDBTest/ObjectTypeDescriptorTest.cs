using System;
using System.Reflection;
using BTDB.EventStoreLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ObjectTypeDescriptorTest
{
    record GoodDto
    {
        public int Id { get; init; }
        public string Name { get; init; }
        [NotStored]
        public string? Description => $"{Id} - {Name}";
    }

    record BadDto
    {
        public int Id { get; init; }
        public string Name { get; init; }
        public string? Description => $"{Id} - {Name}";
    }

    [Fact]
    public void CheckObjectTypeIsGoodDto_GivenRecord_WhenContainsNotStoredGettersAndGeneratedGetters_ShouldIgnoreGeneratedGetters()
    {
        ObjectTypeDescriptor.CheckObjectTypeIsGoodDto(typeof(GoodDto), BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic);
    }

    [Fact]
    public void CheckObjectTypeIsGoodDto_GivenRecord_WhenContainsGettersWithoutNotStoredAttributeAndGeneratedGetters_ShouldThrow()
    {
        Assert.Throws<InvalidOperationException>(() => ObjectTypeDescriptor.CheckObjectTypeIsGoodDto(typeof(BadDto), BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic));
    }
}

