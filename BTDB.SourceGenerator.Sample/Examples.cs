using System;
using System.Reflection;
using BTDB;
using BTDB.IOC;

var builder = new ContainerBuilder();
builder.RegisterType<Klass>();
builder.AutoRegisterTypes().AsSelf();
var container = builder.Build();
container.Resolve<Klass>();
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

class Klass
{
    Klass()
    {
        Console.WriteLine("Ok");
    }
}
