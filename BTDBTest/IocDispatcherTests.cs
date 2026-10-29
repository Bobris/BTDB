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

public class InheritedHandlerMessage
{
}

public abstract class HandlerBase<TMessage> : IDefaultMethodHandler<TMessage>
{
    public List<string> Calls { get; } = [];

    public void Consume(TMessage message) => Calls.Add("base consume");
}

public class InheritedMethodHandler : HandlerBase<InheritedHandlerMessage>
{
}

public class FirstHandlerMessage
{
}

public class SecondHandlerMessage
{
}

public class TwoMessagesHandler : IDefaultMethodHandler<FirstHandlerMessage>,
    IDefaultMethodHandler<SecondHandlerMessage>
{
    public List<string> Calls { get; } = [];

    public void Consume(FirstHandlerMessage message) => Calls.Add("first");

    public void Consume(SecondHandlerMessage message) => Calls.Add("second");
}

public struct StructHandlerMessage
{
    public int Value;
}

public class ValueTypeMessagesHandler : IDefaultMethodHandler<StructHandlerMessage>, IDefaultMethodHandler<long?>
{
    public List<string> Calls { get; } = [];

    public void Consume(StructHandlerMessage message) => Calls.Add("struct " + message.Value);

    public void Consume(long? message) => Calls.Add("long " + message);
}

public class IocDispatcherTests
{
    // Before the fix the generated code cast value type messages with Unsafe.As and did not compile.
    [Fact]
    public unsafe void DispatcherCallsHandlersOfValueTypeMessages()
    {
        var container = BuildContainer();
        var consume = IDefaultMethodDispatcher.CreateConsumeDispatcher(container);

        consume(container, new StructHandlerMessage { Value = 3 });
        consume(container, (long?)4);

        Assert.Equal(["struct 3", "long 4"], container.Resolve<ValueTypeMessagesHandler>().Calls);
    }

    // Before the fix only the first Consume overload was registered, so the other message type had no handler.
    [Fact]
    public unsafe void DispatcherCallsEveryOverloadOfHandler()
    {
        var container = BuildContainer();
        var consume = IDefaultMethodDispatcher.CreateConsumeDispatcher(container);

        consume(container, new FirstHandlerMessage());
        consume(container, new SecondHandlerMessage());

        Assert.Equal(["first", "second"], container.Resolve<TwoMessagesHandler>().Calls);
    }

    // Before the fix a dispatcher method declared in a base class of the handler was not found, so messages of the
    // handler were not dispatched to it.
    [Fact]
    public unsafe void DispatcherCallsMethodOfHandlerBaseClass()
    {
        var container = BuildContainer();
        var consume = IDefaultMethodDispatcher.CreateConsumeDispatcher(container);

        consume(container, new InheritedHandlerMessage());

        Assert.Equal(["base consume"], container.Resolve<InheritedMethodHandler>().Calls);
    }

    [Fact]
    public unsafe void DispatcherCallsDefaultInterfaceMethod()
    {
        var container = BuildContainer();
        var verify = IDefaultMethodDispatcher.CreateVerifyDispatcher(container);
        var consume = IDefaultMethodDispatcher.CreateConsumeDispatcher(container);

        verify(container, new DefaultMethodMessage());
        consume(container, new DefaultMethodMessage());

        Assert.Equal(["default verify", "consume"], container.Resolve<DefaultMethodHandler>().Calls);
    }

    // Before the fix the handlers were kept once per process, so creating a dispatcher for a second container made the
    // dispatcher of the first one call the handlers of the second.
    [Fact]
    public unsafe void DispatcherOfEachContainerCallsItsHandlers()
    {
        var first = BuildContainer();
        var firstConsume = IDefaultMethodDispatcher.CreateConsumeDispatcher(first);
        var second = BuildContainer();
        var secondConsume = IDefaultMethodDispatcher.CreateConsumeDispatcher(second);

        firstConsume(first, new FirstHandlerMessage());
        secondConsume(second, new SecondHandlerMessage());
        firstConsume(first.CreateScope(), new SecondHandlerMessage());

        Assert.Equal(["first", "second"], first.Resolve<TwoMessagesHandler>().Calls);
        Assert.Equal(["second"], second.Resolve<TwoMessagesHandler>().Calls);
    }

    // Creating a dispatcher resolves every handler registered for it, so all of them are registered.
    static IContainer BuildContainer()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<DefaultMethodHandler>().AsSelf().SingleInstance();
        builder.RegisterType<InheritedMethodHandler>().AsSelf().SingleInstance();
        builder.RegisterType<TwoMessagesHandler>().AsSelf().SingleInstance();
        builder.RegisterType<ValueTypeMessagesHandler>().AsSelf().SingleInstance();
        return builder.Build();
    }
}
