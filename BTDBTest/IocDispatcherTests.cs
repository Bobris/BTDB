using System.Collections.Generic;
using BTDB;
using BTDB.IOC;
using Xunit;

namespace BTDBTest;

[Generate]
public partial interface IDefaultMethodDispatcher
{
    public static unsafe partial delegate*<IContainer, object, object?> CreateVerifyDispatcher(IContainer container);
    public static unsafe partial delegate*<IContainer, object, object?> CreateConsumeDispatcher(IContainer container);
}

public interface IDefaultMethodHandler<in TMessage> : IDefaultMethodDispatcher
{
    List<string> Calls { get; }

    void Verify(TMessage message) => Calls.Add("default verify");

    void Consume(TMessage message);
}

public class DefaultMethodMessage
{
}

public class DefaultMethodHandler : IDefaultMethodHandler<DefaultMethodMessage>
{
    public List<string> Calls { get; } = [];

    public void Consume(DefaultMethodMessage message) => Calls.Add("consume");
}

public class IocDispatcherTests
{
    [Fact]
    public unsafe void DispatcherCallsDefaultInterfaceMethod()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<DefaultMethodHandler>().AsSelf().SingleInstance();
        var container = builder.Build();
        var verify = IDefaultMethodDispatcher.CreateVerifyDispatcher(container);
        var consume = IDefaultMethodDispatcher.CreateConsumeDispatcher(container);

        verify(container, new DefaultMethodMessage());
        consume(container, new DefaultMethodMessage());

        Assert.Equal(["default verify", "consume"], container.Resolve<DefaultMethodHandler>().Calls);
    }
}
