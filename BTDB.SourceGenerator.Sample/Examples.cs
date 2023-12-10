using System;
using BTDB;
using BTDB.IOC;

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
    [Dependency]
    public ILogger? Logger
    {
        get => null;
        init => Console.WriteLine(value!.ToString());
    }
}
