using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace BTDB.Replication.Http.Test;

public class ReplicationHostingTest
{
    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    static ReplicationNodeOptions Options => new("cluster", "https://node.example", TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), 1);

    // Manual callbacks keep lease/timeout tests independent of wall-clock scheduling. No timers fire inline.
    sealed class Clock : IReplicationScheduler
    {
        readonly object _lock = new();
        readonly List<Work> _work = new();
        public TimeSpan Elapsed => TimeSpan.Zero;
        public IDisposable Schedule(TimeSpan delay, Action callback, string description)
        {
            var work = new Work(this, callback, description);
            lock (_lock) _work.Add(work);
            return work;
        }
        public void Fire(string description)
        {
            lock (_lock)
            {
                var work = _work.First(w => w.Description == description);
                _work.Remove(work);
                work.Callback();
            }
        }
        public int Pending { get { lock (_lock) return _work.Count; } }
        sealed class Work(Clock owner, Action callback, string description) : IDisposable
        {
            public Action Callback => callback;
            public string Description => description;
            public void Dispose() { lock (owner._lock) owner._work.Remove(this); }
        }
    }

    sealed class Storage : ILeaderRecordStorage, IReplicationLeaseStorage
    {
        readonly object _lock = new();
        LeaderRecord _record = new("0", """
            {"format":1,"clusterId":"cluster","term":0,"revision":0,"applicationGeneration":0,"databaseNames":[]}
            """);
        public int Acquires;
        public bool BlockRenewal;
        public readonly TaskCompletionSource Renewing = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource ReleaseRenewal = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            Acquires++;
            return ValueTask.FromResult<LeaseGrant?>(new("lease", TimeSpan.FromSeconds(30)));
        }
        public async ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation)
        {
            if (BlockRenewal)
            {
                Renewing.TrySetResult();
                await ReleaseRenewal.Task; // Deliberately ignore cancellation to exercise immediate shutdown fencing.
            }
            return TimeSpan.FromSeconds(30);
        }
        public ValueTask<LeaderRecord> ReadAsync(CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            lock (_lock) return ValueTask.FromResult(_record);
        }
        public ValueTask<LeaderWriteOutcome> WriteAsync(string leaseHandle, string expectedToken, string json, CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            lock (_lock)
            {
                if (leaseHandle != "lease" || expectedToken != _record.Token) return ValueTask.FromResult(LeaderWriteOutcome.Rejected);
                _record = new((int.Parse(_record.Token) + 1).ToString(), json);
                return ValueTask.FromResult(LeaderWriteOutcome.Applied);
            }
        }
    }

    sealed class NodeHost : IReplicationNodeHost
    {
        public Func<CancellationToken, Task>? OnRestore;
        public int Restores, Restarts;
        public bool InvalidCandidate;
        public readonly TaskCompletionSource Leader = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource Restoring = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public async ValueTask<IReadOnlyList<ActivationDatabase>> RestoreAsync(CancellationToken cancellation)
        {
            Restores++;
            if (OnRestore != null) await OnRestore(cancellation);
            return [];
        }
        public LeaderCandidate CreateCandidate() => new(InvalidCandidate ? "wrong" : "cluster", "node", "session", 1,
            [], Options.Endpoint, "secret");
        public LeaderTrlProgress? GetProgress(string database) => null;
        public void RequestRestart(string reason) => Restarts++;
        public void ReportStatus(ReplicationNodeRole role)
        {
            if (role == ReplicationNodeRole.Leader) Leader.TrySetResult();
            if (role == ReplicationNodeRole.Restoring) Restoring.TrySetResult();
        }
        public void DatabaseRemoved(string database) { }
    }

    sealed class RunningHost : IAsyncDisposable
    {
        public readonly Clock Clock = new();
        public readonly Storage Storage = new();
        public readonly NodeHost Node = new();
        public readonly WebApplication App;
        public IHostApplicationLifetime Lifetime => App.Services.GetRequiredService<IHostApplicationLifetime>();
        public LeaseSessionController Leases => App.Services.GetRequiredService<LeaseSessionController>();
        public ReplicationNodeCoordinator Coordinator => App.Services.GetRequiredService<ReplicationNodeCoordinator>();
        public ReplicationHostedService Worker => App.Services.GetServices<IHostedService>().OfType<ReplicationHostedService>().Single();
        public string Address => App.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single();
        public RunningHost(bool map = true)
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseKestrel().UseUrls("http://127.0.0.1:0");
            // Even Ignore must not leave a live HTTP process after losing its replication worker.
            builder.Services.Configure<HostOptions>(o => o.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore);
            builder.Services.AddSingleton<IReplicationScheduler>(Clock);
            builder.Services.AddSingleton<IReplicationLeaseStorage>(Storage);
            builder.Services.AddSingleton<ILeaderRecordStorage>(Storage);
            builder.Services.AddSingleton<IReplicationNodeHost>(Node);
            builder.Services.AddBTDBReplication(Options, 0, TimeSpan.Zero);
            App = builder.Build();
            if (map) App.MapBTDBReplication();
        }
        public async Task StartLeader()
        {
            await App.StartAsync();
            await Node.Leader.Task.WaitAsync(Timeout);
        }
        public async ValueTask DisposeAsync()
        {
            Storage.ReleaseRenewal.TrySetResult();
            using var deadline = new CancellationTokenSource(Timeout);
            await App.StopAsync(deadline.Token);
            await App.DisposeAsync();
        }
    }

    [Fact]
    public async Task StartsAfterKestrelAndServesTheRealCoordinator()
    {
        await using var host = new RunningHost();
        host.Node.OnRestore = async cancellation =>
        {
            Assert.True(host.Lifetime.ApplicationStarted.IsCancellationRequested);
            using var probe = new HttpClient();
            using var response = await probe.PostAsync(host.Address + HttpReplicationPeerTransport.Path, null, cancellation);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode); // Listening, but restore has not registered a leader.
        };
        await host.StartLeader();
        using var client = new HttpClient();
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", "secret");
        var request = new HttpReplicationPeerTransport.Request("cluster", 1, "session", Options.Endpoint,
            "poll", Challenge: 3, DurationTicks: TimeSpan.FromSeconds(1).Ticks);
        using var reply = await client.PostAsJsonAsync(host.Address + HttpReplicationPeerTransport.Path, request,
            new System.Text.Json.JsonSerializerOptions());
        Assert.Equal(HttpStatusCode.OK, reply.StatusCode);
        var progress = await reply.Content.ReadFromJsonAsync<ReplicationPeerProgress>();
        Assert.Equal(new ReplicationPeerProgress(3, true, null), progress);
        Assert.Equal(1, host.Storage.Acquires);
        host.Lifetime.StopApplication();
        Assert.Null(host.Leases.Current);
        await host.Worker.ExecuteTask!.WaitAsync(Timeout);
        Assert.Equal(ReplicationNodeRole.Stopped, host.Coordinator.Role);
        Assert.Equal(0, host.Clock.Pending);
    }

    [Fact]
    public async Task ShutdownFencesBeforeAnUncooperativeRenewalReturns()
    {
        await using var host = new RunningHost();
        await host.StartLeader();
        var authority = host.Leases.Current!;
        host.Storage.BlockRenewal = true;
        host.Clock.Fire("lease maintenance");
        await host.Storage.Renewing.Task.WaitAsync(Timeout);
        host.Lifetime.StopApplication();
        Assert.False(authority.IsValid);
        Assert.Null(host.Leases.Current);
        Assert.False(host.Worker.ExecuteTask!.IsCompleted);
        host.Storage.ReleaseRenewal.SetResult();
        await host.Worker.ExecuteTask.WaitAsync(Timeout);
        Assert.Null(host.Leases.Current);
        Assert.Equal(1, host.Storage.Acquires);
        Assert.Equal(ReplicationNodeRole.Stopped, host.Coordinator.Role);
        Assert.Equal(0, host.Clock.Pending);
    }

    [Fact]
    public async Task RestartRequiredStopsTheHostInsteadOfLeavingItsEndpointRunning()
    {
        await using var host = new RunningHost();
        host.Node.InvalidCandidate = true;
        await host.App.StartAsync();
        await host.Worker.ExecuteTask!.WaitAsync(Timeout);
        Assert.True(host.Lifetime.ApplicationStopping.IsCancellationRequested);
        Assert.Equal(1, host.Node.Restarts);
        Assert.Equal(ReplicationNodeRole.RestartRequired, host.Coordinator.Role);
        Assert.Null(host.Leases.Current);
    }

    [Fact]
    public async Task FatalRestoreFailureStopsHostEvenWithIgnoreBackgroundFailurePolicy()
    {
        await using var host = new RunningHost();
        host.Node.OnRestore = _ => throw new InvalidOperationException("Restore contract failed.");
        await host.App.StartAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => host.Worker.ExecuteTask!.WaitAsync(Timeout));
        Assert.True(host.Lifetime.ApplicationStopping.IsCancellationRequested);
        Assert.Equal(0, host.Storage.Acquires);
        Assert.Null(host.Leases.Current);
    }

    [Fact]
    public async Task FailedRestoreRetriesWithoutContendingForLeadership()
    {
        await using var host = new RunningHost();
        host.Node.OnRestore = _ => host.Node.Restores == 1 ? throw new IOException("Retry restore.") : Task.CompletedTask;
        await host.App.StartAsync();
        await host.Node.Restoring.Task.WaitAsync(Timeout);
        Assert.Equal(0, host.Storage.Acquires);
        host.Clock.Fire("replication node poll");
        await host.Node.Leader.Task.WaitAsync(Timeout);
        Assert.Equal(2, host.Node.Restores);
        Assert.Equal(1, host.Storage.Acquires);
    }

    [Fact]
    public async Task StopDuringRestoreCancelsItAndNeverAcquiresALease()
    {
        await using var host = new RunningHost();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        host.Node.OnRestore = async cancellation =>
        {
            entered.SetResult();
            await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, cancellation);
        };
        await host.App.StartAsync();
        await entered.Task.WaitAsync(Timeout);
        using var deadline = new CancellationTokenSource(Timeout);
        await host.App.StopAsync(deadline.Token);
        await host.Worker.ExecuteTask!.WaitAsync(Timeout);
        Assert.Equal(0, host.Storage.Acquires);
        Assert.Equal(ReplicationNodeRole.Stopped, host.Coordinator.Role);
    }

    [Fact]
    public async Task MissingPeerRouteFailsStartupBeforeLeaseAcquisition()
    {
        await using var host = new RunningHost(false);
        await Assert.ThrowsAsync<InvalidOperationException>(() => host.App.StartAsync());
        Assert.Equal(0, host.Node.Restores);
        Assert.Equal(0, host.Storage.Acquires);
    }

    [Fact]
    public async Task DuplicateRouteMappingIsRejected()
    {
        await using var host = new RunningHost();
        Assert.Throws<InvalidOperationException>(() => host.App.MapBTDBReplication());
    }

    [Fact]
    public void InvalidConfigurationFailsBeforeBuildingTheHost()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentException>(() => services.AddBTDBReplication(Options with { Endpoint = "http://node.example" }, 0, TimeSpan.Zero));
        Assert.Throws<ArgumentException>(() => services.AddBTDBReplication(Options with { ClusterId = " " }, 0, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddBTDBReplication(Options with { CompactionInterval = TimeSpan.Zero }, 0, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddBTDBReplication(Options, 1_000_000, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddBTDBReplication(Options, 0, TimeSpan.FromTicks(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddBTDBReplication(Options, 0, TimeSpan.Zero, 0));
        services.AddBTDBReplication(Options, 0, TimeSpan.Zero);
        Assert.Throws<InvalidOperationException>(() => services.AddBTDBReplication(Options, 0, TimeSpan.Zero));
    }
}
