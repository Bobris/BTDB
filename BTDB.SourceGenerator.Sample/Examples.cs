using System;
using BTDB;
using BTDB.IOC;

var builder = new ContainerBuilder();
builder.AutoRegisterTypes().AsSelf();
var container = builder.Build();
unsafe
{
    var h = IAnyHandler.CreateConsumeDispatcher(container);
    h(container, "Hello");
}

[Generate]
public partial interface IAnyHandler
{
    public static unsafe partial delegate*<IContainer, object, object?> CreateConsumeDispatcher(IContainer container);
}

class Example : IAnyHandler
{
    internal void Consume(string message)
    {
        Console.WriteLine(message);
    }
}
