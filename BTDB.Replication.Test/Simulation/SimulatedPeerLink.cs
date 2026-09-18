using System;

namespace BTDB.Replication.Test.Simulation;

// An opaque-byte fault fixture only. It cannot claim transport conformance before the M1/M4 wire codec exists.
internal sealed class SimulatedPeerLink
{
    readonly DeterministicScheduler.Scope _receiver;
    readonly int _capacity;
    int _queued;
    long _connection;
    bool _connected = true;

    public SimulatedPeerLink(DeterministicScheduler.Scope receiver, int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        _receiver = receiver;
        _capacity = capacity;
    }

    public void Disconnect()
    {
        _connected = false;
        _connection++;
        _queued = 0;
    }

    public void Reconnect() => _connected = true;

    public bool TrySend(ReadOnlySpan<byte> bytes, TimeSpan delay, Action<ReadOnlyMemory<byte>> receive)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(delay.Ticks);
        if (!_connected || _receiver.Stopped || _queued >= _capacity) return false;
        var copy = bytes.ToArray();
        var connection = _connection;
        _queued++;
        _receiver.Schedule(delay, () =>
        {
            if (_connection != connection) return;
            _queued--;
            if (_connected) receive(copy);
        }, $"peer delivery connection={connection} bytes={copy.Length}");
        return true;
    }
}
