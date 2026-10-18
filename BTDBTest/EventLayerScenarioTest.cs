using System.Linq;
using Xunit;

namespace BTDBTest;

public class EventLayerScenarioNoEmitTest
{
    public static TheoryData<string, EventLayer> Cases()
    {
        var data = new TheoryData<string, EventLayer>();
        foreach (var name in EventLayerScenarios.All.Keys.OrderBy(n => n))
        foreach (var layer in new[] { EventLayer.Mapping, EventLayer.Store1, EventLayer.Store2 })
            data.Add(name, layer);
        return data;
    }

    internal static void Run(string scenarioName, EventLayer layer)
    {
        var scenario = EventLayerScenarios.All[scenarioName];
        var original = scenario.Create();
        var result = EventLayerRoundTripHelper.RoundTrip(layer, original, scenario.WriteMapper?.Invoke(),
            scenario.ReadMapper?.Invoke(), EventLayerScenarios.CipherFor(scenario));
        if (scenario.Verify != null)
        {
            scenario.Verify(original, result);
        }
        else
        {
            Assert.Equal(ObjectGraphDumper.Dump(original), ObjectGraphDumper.Dump(result));
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void RoundTrip(string scenario, EventLayer layer)
    {
        Run(scenario, layer);
    }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class EventLayerScenarioEmitTest
{
    public static TheoryData<string, EventLayer> Cases()
    {
        var data = new TheoryData<string, EventLayer>();
        // IL generated loaders cannot convert item types of collections (e.g. List<int> to long[])
        foreach (var name in EventLayerScenarios.All.Keys.Where(n => n != "ChangedFieldTypes").OrderBy(n => n))
        foreach (var layer in new[] { EventLayer.Mapping, EventLayer.Store1, EventLayer.Store2 })
            data.Add(name, layer);
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void RoundTrip(string scenario, EventLayer layer)
    {
        EventLayerRoundTripHelper.RunWithEmitMode(false, () => EventLayerScenarioNoEmitTest.Run(scenario, layer));
    }
}
