using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Xunit;

namespace BTDB.Replication.Azure.Test;

public sealed class AzuriteFixture : IAsyncLifetime
{
    Process _process = null!;
    string _directory = null!;
    Uri _endpoint = null!;
    Task<string> _output = null!, _error = null!;
    readonly StorageSharedKeyCredential _credential = new("test", Convert.ToBase64String(new byte[32]));

    public async Task InitializeAsync()
    {
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
        var options = new BlobClientOptions();
        options.Retry.MaxRetries = 0;
        if (policy != null) options.AddPolicy(policy, global::Azure.Core.HttpPipelinePosition.PerCall);
        var service = new BlobServiceClient(_endpoint, _credential, options);
        var container = service.GetBlobContainerClient("test-" + Guid.NewGuid().ToString("N"));
        await container.CreateAsync();
        return container;
    }

    public async Task DisposeAsync()
    {
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
