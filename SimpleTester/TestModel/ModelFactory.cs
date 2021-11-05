using ProtoBuf.Meta;
using SimpleTester.TestModel.Events;

namespace SimpleTester.TestModel;

public static class ModelFactory
{
    public static RuntimeTypeModel CreateModel()
    {
        var model = RuntimeTypeModel.Create();

        model.Add(typeof(Events.Event), true)
            .AddSubType(100, typeof(ActionFinishedBase<string>))
            .AddSubType(101, typeof(NewUserEvent));

        model.Add(typeof(ActionFinishedBase<string>), true)
            .AddSubType(100, typeof(TemplateSavedV1));

        return model;
    }
}
