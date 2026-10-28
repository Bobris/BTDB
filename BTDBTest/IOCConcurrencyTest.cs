using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB;
using BTDB.IOC;
using Xunit;

namespace BTDBTest;

/// Singletons resolved by many threads at once are constructed once per container, also through a dependency.
public class IOCConcurrencyTest
{
    public interface IInner;

    public interface IOuter
    {
        IInner Inner { get; }
    }

    [Generate]
    public class Inner : IInner
    {
        public static int Constructed;

        public Inner()
        {
            Interlocked.Increment(ref Constructed);
            Thread.SpinWait(2000);
        }
    }

    [Generate]
    public class Outer : IOuter
    {
        public static int Constructed;

        public Outer(IInner inner)
        {
            Inner = inner;
            Interlocked.Increment(ref Constructed);
            Thread.SpinWait(2000);
        }

        public IInner Inner { get; }
    }

    [Fact]
    public async Task SingletonsResolvedConcurrentlyAreConstructedOnce()
    {
        const int threads = 16;
        for (var round = 0; round < 100; round++)
        {
            Inner.Constructed = 0;
            Outer.Constructed = 0;
            var builder = new ContainerBuilder();
            builder.RegisterType<Inner>().As<IInner>().SingleInstance();
            builder.RegisterType<Outer>().As<IOuter>().SingleInstance();
            var container = builder.Build();
            using var barrier = new Barrier(threads);
            var results = Enumerable.Range(0, threads).Select(i => Task.Factory.StartNew(() =>
            {
                barrier.SignalAndWait();
                return i % 2 == 0 ? container.Resolve<IOuter>() : (object)container.Resolve<IInner>();
            }, TaskCreationOptions.LongRunning)).ToArray();
            await Task.WhenAll(results);
            var outers = results.Select(t => t.Result).OfType<IOuter>().Distinct().ToList();
            var inners = results.Select(t => t.Result).OfType<IInner>().Concat(outers.Select(o => o.Inner)).Distinct()
                .ToList();
            Assert.Single(outers);
            Assert.Single(inners);
            Assert.Equal(1, Outer.Constructed);
            Assert.Equal(1, Inner.Constructed);
        }
    }

    public interface IScopedThing;

    [Generate]
    public class ScopedThing : IScopedThing
    {
        public static int Constructed;

        public ScopedThing()
        {
            Interlocked.Increment(ref Constructed);
            Thread.SpinWait(2000);
        }
    }

    /// Scoped services resolved by many threads in several scopes at once are constructed once per scope.
    [Fact]
    public async Task ScopedServicesResolvedConcurrentlyAreConstructedOncePerScope()
    {
        const int scopes = 4;
        const int threadsPerScope = 6;
        for (var round = 0; round < 50; round++)
        {
            ScopedThing.Constructed = 0;
            var builder = new ContainerBuilder();
            builder.RegisterType<ScopedThing>().As<IScopedThing>().Scoped();
            var container = builder.Build();
            var created = Enumerable.Range(0, scopes).Select(_ => container.CreateScope()).ToArray();
            using var barrier = new Barrier(scopes * threadsPerScope);
            var results = Enumerable.Range(0, scopes * threadsPerScope).Select(i => Task.Factory.StartNew(() =>
            {
                barrier.SignalAndWait();
                return (Scope: i % scopes, Thing: created[i % scopes].Resolve<IScopedThing>());
            }, TaskCreationOptions.LongRunning)).ToArray();
            await Task.WhenAll(results);
            var perScope = results.Select(t => t.Result).GroupBy(r => r.Scope)
                .Select(g => g.Select(r => r.Thing).Distinct().ToList()).ToList();
            Assert.All(perScope, things => Assert.Single(things));
            Assert.Equal(scopes, perScope.Select(things => things[0]).Distinct().Count());
            Assert.Equal(scopes, ScopedThing.Constructed);
            foreach (var scope in created) await scope.DisposeAsync();
        }
    }

    public interface IResource;

    public interface IUser;

    public static class DisposalLog
    {
        public static readonly System.Collections.Concurrent.ConcurrentQueue<string> Entries = new();
    }

    [Generate]
    public class Resource : IResource, System.IDisposable
    {
        public void Dispose() => DisposalLog.Entries.Enqueue("resource");
    }

    [Generate]
    public class ScopedUser : IUser, System.IAsyncDisposable
    {
        public ScopedUser(IResource resource)
        {
        }

        public ValueTask DisposeAsync()
        {
            DisposalLog.Entries.Enqueue("user");
            return ValueTask.CompletedTask;
        }
    }

    /// A scope disposes its scoped instances once, also those created by many threads at once, and leaves singletons
    /// to the container, which disposes them after the instances that depend on them.
    [Fact]
    public async Task ScopesAndContainerDisposeWhatTheyOwnOnce()
    {
        while (DisposalLog.Entries.TryDequeue(out _))
        {
        }

        var builder = new ContainerBuilder();
        builder.RegisterType<Resource>().As<IResource>().SingleInstance();
        builder.RegisterType<ScopedUser>().As<IUser>().Scoped();
        var container = builder.Build();
        var scope = container.CreateScope();
        using (var barrier = new Barrier(8))
            await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Factory.StartNew(() =>
            {
                barrier.SignalAndWait();
                return scope.Resolve<IUser>();
            }, TaskCreationOptions.LongRunning)));
        await scope.DisposeAsync();
        await scope.DisposeAsync();
        Assert.Equal(new[] { "user" }, DisposalLog.Entries.ToArray());
        container.Resolve<IUser>();
        await container.DisposeAsync();
        await container.DisposeAsync();
        Assert.Equal(new[] { "user", "user", "resource" }, DisposalLog.Entries.ToArray());
    }
}
