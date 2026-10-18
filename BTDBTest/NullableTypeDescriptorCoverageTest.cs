using System;
using BTDB;
using BTDB.EventStoreLayer;
using Xunit;
using static BTDBTest.EventLayerRoundTripHelper;

namespace BTDBTest;

public class NullableTypeDescriptorCoverageTest
{
    [Generate]
    public class LargeNullables
    {
        public decimal? Dec { get; set; }
        public decimal? NullDec { get; set; }
        public Guid? G { get; set; }
        public DateTime? Dt { get; set; }
        public int? I { get; set; }
        public (int, string)? T { get; set; }
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void NullablesCanBeReadDynamically(EventLayer layer)
    {
        var g = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        var dt = new DateTime(2000, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var value = new LargeNullables { Dec = 1.5m, G = g, Dt = dt, I = 3, T = (4, "four") };
        var readMapper = new RenamingTypeMapper().Hide(new FullNameTypeMapper().ToName(typeof(LargeNullables)));
        var fields = EventLayerScenarios.Fields(RoundTrip(layer, value, readMapper: readMapper));
        Assert.Equal(1.5m, fields["Dec"]);
        Assert.Null(fields["NullDec"]);
        Assert.Equal(g, fields["G"]);
        Assert.Equal(dt, fields["Dt"]);
        Assert.Equal(3, fields["I"]);
        Assert.Equal((4, "four"), fields["T"]);
    }

    [Generate]
    public class NullablesV2
    {
        public decimal? Dec { get; set; }
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void RemovedNullableFieldsAreSkipped(EventLayer layer)
    {
        var value = new LargeNullables { Dec = 2.5m, G = Guid.NewGuid(), I = 3, T = (4, "four") };
        var result = RoundTrip(layer, value,
            new RenamingTypeMapper().Map(typeof(LargeNullables), "N"),
            new RenamingTypeMapper().Map(typeof(NullablesV2), "N"));
        Assert.Equal(2.5m, Assert.IsType<NullablesV2>(result).Dec);
    }
}
