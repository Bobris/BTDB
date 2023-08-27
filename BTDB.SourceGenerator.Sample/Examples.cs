using System;
using BTDB;
using BTDB.IOC;

var builder = new ContainerBuilder();
builder.AutoRegisterTypes().AsSelf();
var container = builder.Build();
var example = container.Resolve<Example>();
Console.WriteLine(example.ToString());

partial interface IAnyHandler
{
    //public static unsafe partial delegate*<IContainer, object, object?> CreateConsumer(IContainer container);
}

[Generate]
class Example
{
}
