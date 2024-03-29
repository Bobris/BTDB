using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB;
using BTDB.IOC;
using BTDB.Serialization;
using Microsoft.AspNetCore.Http;

var builder = new ContainerBuilder();
//builder.RegisterType<Klass>();
builder.AutoRegisterTypes().AsSelf();
var container = builder.Build();
container.Resolve<Klass>();
unsafe
{
    var h = IAnyHandler.CreateConsumeDispatcher(container);
    h(container, "Hello");
}

unsafe
{
    ReflectionMetadata.RegisterCollection(new()
    {
        Type = typeof(Dictionary<int, string>),
        ElementKeyType = typeof(int),
        ElementValueType = typeof(string),
        Creator = &Create1,
        AdderKeyValue = &Add1
    });
}

static object Create1(uint capacity)
{
    return new Dictionary<int, string>((int)capacity);
}

static void Add1(object c, ref byte key, ref byte value)
{
    Unsafe.As<Dictionary<int, string>>(c).Add(Unsafe.As<byte, int>(ref key), Unsafe.As<byte, string>(ref value));
}

[Generate]
public class DynamicEventWrapper
{
    public dynamic DynamicEvent { get; set; }

    public DynamicEventWrapper(dynamic dynamicEvent)
    {
        DynamicEvent = dynamicEvent;
    }
}

[Generate]
public class Person
{
    string _name = "";
    int _age;

    public string Name
    {
        get => _name;
        set => _name = value;
    }

    public int Age
    {
        get => _age;
        set => _age = value + 1;
    }
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

[Generate]
public class Klass
{
    private Klass()
    {
        Console.WriteLine("Ok");
    }
}

public interface ILogger
{
}

[Generate]
public class ErrorHandler
{
    [BTDB.IOC.Dependency]
    public ILogger? Logger
    {
        get => null;
        init => Console.WriteLine(value!.ToString());
    }
}
