# Features of the Source Generator

## IOC Container

### Classes

BTDB IOC Container needs Factories for all types that should be constructable.
Mark type or any of its base classes or interfaces by `[Generate]` attribute.
Constructor with most parameters will be used for construction.
If that constructor is private, then type must be partial including all its outer types.
Type must be public or internal.

```csharp
[Generate]
public class MyService
{
    public MyService(int a, string b)
    {
    }
}
```

```csharp
[Generate]
public interface IMyService
{
}

public class MyService : IMyService
{
    public MyService(int a, string b)
    {
    }
}
```

All injectable properties must be marked by `[Dependency]` attribute. If property is initonly then it is always set even when it is optional.

```csharp
internal class MyService : IMyService
{
    [Dependency]
    internal ILogger Logger { get; init; }
}
```

If property name is different from named registration You can define named resolve of property by using `[Dependency("name")]` attribute.

### Delegates and Func<...>

If you want to make resolvable some delegate mark its declaration by `[Generate]` attribute.

```csharp
[Generate]
public delegate IMyService Factory(int a);
```

If you want to make Func<...> resolvable just declare new delegate with same signature and mark it by `[Generate]` attribute.
It means that previous example also allows to use `Func<int, IMyService>`.
