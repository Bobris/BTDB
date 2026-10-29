using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using BTDB.IOC;
using Xunit;

namespace BTDBTest;

// A scope disposed while another thread resolves from it must still dispose every instance it created.
public class IOCDisposeRaceTest
{
    public sealed class TrackedService : IDisposable
    {
        public static readonly ConcurrentBag<TrackedService> Created = [];
        public int Disposed;

        public TrackedService() => Created.Add(this);

        public void Dispose() => Interlocked.Increment(ref Disposed);
    }

    // Before the fix an instance created while the scope was being disposed could miss the disposal, either because it
    // was added after disposal took the instances, or because adding it threw before disposing it.
    [Fact]
    public void InstancesCreatedDuringDisposalAreDisposed()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback<TrackedService>().AsSelf().Scoped();
        var container = builder.Build();
        for (var i = 0; i < 20000; i++)
        {
            var scope = container.CreateScope();
            using var start = new Barrier(2);
            var resolve = Task.Run(() =>
            {
                start.SignalAndWait();
                try
                {
                    scope.Resolve<TrackedService>();
                }
                catch (ObjectDisposedException)
                {
                }
            });
            start.SignalAndWait();
            scope.DisposeAsync().AsTask().GetAwaiter().GetResult();
            resolve.GetAwaiter().GetResult();
        }

        foreach (var service in TrackedService.Created) Assert.Equal(1, service.Disposed);
    }
}
