using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Blobs;
using Xunit;

namespace BTDB.Replication.Azure.Test;

/// <summary>Blob service for the Azure adapter suites: a private loopback Azurite by default, or a live Azure
/// account when BTDB_AZURE_BLOB_ENDPOINT names its blob endpoint (authenticated by DefaultAzureCredential, which
/// needs a Storage Blob Data Contributor role). Live runs create one container per test and delete them all.</summary>
public sealed class BlobStorageFixture : IAsyncLifetime
{
    Process? _process;
    string? _directory;
    Uri _endpoint = null!;
    Task<string> _output = null!, _error = null!;
    readonly StorageSharedKeyCredential _credential = new("test", Convert.ToBase64String(new byte[32]));
    TokenCredential? _live;
    readonly ConcurrentBag<string> _created = new();

    public bool IsLive => _live != null;

    public async Task InitializeAsync()
    {
        if (Environment.GetEnvironmentVariable("BTDB_AZURE_BLOB_ENDPOINT") is { Length: > 0 } live)
        {
            _endpoint = new(live);
            _live = new DefaultAzureCredential();
            return;
        }
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        _directory = Directory.CreateTempSubdirectory("btdb-azurite-").FullName;
        _endpoint = new($"http://127.0.0.1:{port}/test");
        var executable = Environment.GetEnvironmentVariable("BTDB_AZURITE_EXECUTABLE") ?? "azurite-blob";
        var start = new ProcessStartInfo(OperatingSystem.IsWindows() ? "cmd.exe" : executable)
        {
            RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false
        };
        if (OperatingSystem.IsWindows()) { start.ArgumentList.Add("/c"); start.ArgumentList.Add(executable); }
        foreach (var arg in new[] { "--blobHost", "127.0.0.1", "--blobPort", port.ToString(), "--location", _directory,
                     "--silent", "--skipApiVersionCheck", "--disableTelemetry" }) start.ArgumentList.Add(arg);
        start.Environment["AZURITE_ACCOUNTS"] = "test:" + Convert.ToBase64String(new byte[32]);
        _process = Process.Start(start)!;
        _output = _process.StandardOutput.ReadToEndAsync();
        _error = _process.StandardError.ReadToEndAsync();
        for (var i = 0; i < 100; i++)
        {
            if (_process.HasExited) throw new InvalidOperationException(await _error + await _output);
            try
            {
                using var probe = new TcpClient();
                await probe.ConnectAsync(IPAddress.Loopback, port);
                return;
            }
            catch (SocketException) { await Task.Delay(50); }
        }
        throw new TimeoutException("Azurite did not start.");
    }

    internal async Task<BlobContainerClient> ContainerAsync(HttpPipelinePolicy? policy = null)
    {
        var name = "btdb-test-" + Guid.NewGuid().ToString("N");
        _created.Add(name);
        var container = Connect(name, policy);
        await container.CreateAsync();
        return container;
    }

    /// <summary>Another client of an existing container, as a separate node with its own request faults sees it.
    /// On live Azure, wrapCredential can replace the node's token credential (for example to force token refreshes).</summary>
    internal BlobContainerClient Connect(string container, HttpPipelinePolicy? policy = null,
        Func<TokenCredential, TokenCredential>? wrapCredential = null)
    {
        var options = new BlobClientOptions();
        options.Retry.MaxRetries = 0;
        if (policy != null) options.AddPolicy(policy, HttpPipelinePosition.PerCall);
        var service = _live != null ? new BlobServiceClient(_endpoint, wrapCredential?.Invoke(_live) ?? _live, options)
            : new BlobServiceClient(_endpoint, _credential, options);
        return service.GetBlobContainerClient(container);
    }

    public async Task DisposeAsync()
    {
        if (_live != null)
            foreach (var name in _created)
                try { await Connect(name).DeleteIfExistsAsync(); }
                catch (Exception) { } // Best effort: a lifecycle rule or a later run removes leftovers.
        if (_process != null)
        {
            if (!_process.HasExited) _process.Kill(true);
            await _process.WaitForExitAsync();
            await Task.WhenAll(_output, _error);
            _process.Dispose();
        }
        if (_directory != null) Directory.Delete(_directory, true);
    }
}
