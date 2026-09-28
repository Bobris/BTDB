using System;
using System.Buffers;
using System.Security.Cryptography;
using System.Threading;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

internal static class FileChecksum
{
    internal static string Compute(IFileCollectionFile file, ulong length, int bufferSize, CancellationToken cancellation)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            for (ulong offset = 0; offset < length;)
            {
                cancellation.ThrowIfCancellationRequested();
                var count = (int)Math.Min((ulong)bufferSize, length - offset);
                file.RandomRead(buffer.AsSpan(0, count), offset, false);
                hash.AppendData(buffer, 0, count);
                offset += (uint)count;
            }
            cancellation.ThrowIfCancellationRequested();
            return Convert.ToHexString(hash.GetHashAndReset());
        }
        finally { ArrayPool<byte>.Shared.Return(buffer); }
    }
}
