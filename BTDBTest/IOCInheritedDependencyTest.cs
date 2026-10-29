using BTDB;
using BTDB.IOC;
using BTDBTest.IOCDomain;
using Xunit;

namespace BTDBTest;

public class IOCInheritedDependencyTest
{
    public class DependenciesBase
    {
        [Dependency] public ILogger Public { get; set; } = null!;
        [Dependency] public ILogger PrivateSetter { get; private set; } = null!;
        [Dependency] public ILogger InitOnly { get; init; } = null!;
        [Dependency] public ILogger ProtectedSetter { get; protected set; } = null!;
        [Dependency] public virtual ILogger Overridden { get; set; } = null!;
        [Dependency("Named")] public ILogger Renamed { get; set; } = null!;
        [Dependency] ILogger _privateField = null!;
        [Dependency] protected ILogger ProtectedField = null!;
        [Dependency] public ILogger? Optional { get; set; }

        public ILogger PrivateField => _privateField;
    }

    [Generate]
    public class WithInheritedDependencies : DependenciesBase
    {
        public override ILogger Overridden { get; set; } = null!;
        public ILogger ProtectedFieldValue => ProtectedField;
    }

    [Generate]
    public partial class PartialWithInheritedDependencies : DependenciesBase
    {
        [Dependency] public IErrorHandler Own { get; set; } = null!;
        public ILogger ProtectedFieldValue => ProtectedField;
    }

    public class GenericDependenciesBase<T>
    {
        [Dependency] public T Value { get; private set; } = default!;
        [Dependency] T _field = default!;

        public T Field => _field;
    }

    [Generate]
    public class WithGenericBase : GenericDependenciesBase<ILogger>
    {
    }

    static IContainer BuildContainer<T>(out Logger logger, out Logger named) where T : class
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<T>().AsSelf();
        logger = new Logger();
        named = new Logger();
        builder.RegisterInstance<ILogger>(logger);
        builder.RegisterInstance<ILogger>(named).Named<ILogger>("Named");
        builder.RegisterType<ErrorHandler>().As<IErrorHandler>();
        return builder.Build();
    }

    static void AssertInjected(DependenciesBase obj, ILogger protectedField, Logger logger, Logger named)
    {
        Assert.Same(logger, obj.Public);
        Assert.Same(logger, obj.PrivateSetter);
        Assert.Same(logger, obj.InitOnly);
        Assert.Same(logger, obj.ProtectedSetter);
        Assert.Same(logger, obj.Overridden);
        Assert.Same(named, obj.Renamed);
        Assert.Same(logger, obj.PrivateField);
        Assert.Same(logger, protectedField);
        Assert.Same(logger, obj.Optional);
    }

    [Fact]
    public void InjectsDependenciesDeclaredInBaseClass()
    {
        var container = BuildContainer<WithInheritedDependencies>(out var logger, out var named);
        var obj = container.Resolve<WithInheritedDependencies>();
        AssertInjected(obj, obj.ProtectedFieldValue, logger, named);
    }

    [Fact]
    public void InjectsDependenciesDeclaredInBaseClassOfPartialClass()
    {
        var container = BuildContainer<PartialWithInheritedDependencies>(out var logger, out var named);
        var obj = container.Resolve<PartialWithInheritedDependencies>();
        AssertInjected(obj, obj.ProtectedFieldValue, logger, named);
        Assert.Same(logger, obj.Own.Logger);
    }

    [Fact]
    public void InjectsDependenciesDeclaredInGenericBaseClass()
    {
        var container = BuildContainer<WithGenericBase>(out var logger, out _);
        var obj = container.Resolve<WithGenericBase>();
        Assert.Same(logger, obj.Value);
        Assert.Same(logger, obj.Field);
    }
}
