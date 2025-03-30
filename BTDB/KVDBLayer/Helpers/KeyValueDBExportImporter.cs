using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using BTDB.Buffer;

namespace BTDB.KVDBLayer;

public static class KeyValueDBExportImporter
{
    /// <summary>
    /// Writes all key value pairs to stream
    /// </summary>
    /// <param name="transaction">transaction from where export all data</param>
    /// <param name="stream">where to write it to</param>
    public static async Task ExportAsync(IKeyValueDBTransaction transaction, Stream stream)
    {
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));
        if (stream == null) throw new ArgumentNullException(nameof(stream));
        if (!stream.CanWrite) throw new ArgumentException("stream must be writeable", nameof(stream));
        var keyValueCount = transaction.GetKeyValueCount();
        using var cursor = transaction.CreateCursor();
        var keyBuf = Memory<byte>.Empty;
        var valueBuf = Memory<byte>.Empty;
        var tempBuf = new byte[16];
        "BTDBEXP2"u8.CopyTo(tempBuf);
        PackUnpack.PackInt64LE(tempBuf, 8, keyValueCount);
        await stream.WriteAsync(tempBuf.AsMemory(0, 16));
        cursor.FindFirstKey(new());
        for (long kv = 0; kv < keyValueCount; kv++)
        {
            var key = cursor.GetKeyMemory(ref keyBuf);
            PackUnpack.PackInt32LE(tempBuf, 0, key.Length);
            await stream.WriteAsync(tempBuf.AsMemory(0, 4));
            await stream.WriteAsync(key);
            var value = cursor.GetValueMemory(ref valueBuf);
            PackUnpack.PackInt32LE(tempBuf, 0, value.Length);
            await stream.WriteAsync(tempBuf.AsMemory(0, 4));
            await stream.WriteAsync(value);
            cursor.FindNextKey(new());
        }

        var ulongCount = transaction.GetUlongCount();
        if (transaction.GetCommitUlong() != 0 || ulongCount != 0)
        {
            PackUnpack.PackUInt64LE(tempBuf, 0, transaction.GetCommitUlong());
            await stream.WriteAsync(tempBuf.AsMemory(0, 8));
        }

        if (ulongCount != 0)
        {
            PackUnpack.PackUInt32LE(tempBuf, 0, ulongCount);
            await stream.WriteAsync(tempBuf.AsMemory(0, 4));
            for (var i = 0u; i < ulongCount; i++)
            {
                PackUnpack.PackUInt64LE(tempBuf, 0, transaction.GetUlong(i));
                await stream.WriteAsync(tempBuf.AsMemory(0, 8));
            }
        }
    }

    /// <summary>
    /// Writes all key value pairs to stream
    /// </summary>
    /// <param name="transaction">transaction from where export all data</param>
    /// <param name="stream">where to write it to</param>
    [SkipLocalsInit]
    public static void Export(IKeyValueDBTransaction transaction, Stream stream)
    {
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));
        if (stream == null) throw new ArgumentNullException(nameof(stream));
        if (!stream.CanWrite) throw new ArgumentException("stream must be writeable", nameof(stream));
        var keyValueCount = transaction.GetKeyValueCount();
        using var cursor = transaction.CreateCursor();
        var tempBuf = new byte[16];
        "BTDBEXP2"u8.CopyTo(tempBuf);
        PackUnpack.PackInt64LE(tempBuf, 8, keyValueCount);
        stream.Write(tempBuf, 0, 16);
        cursor.FindFirstKey(new());
        Span<byte> buffer = stackalloc byte[512];
        for (long kv = 0; kv < keyValueCount; kv++)
        {
            var key = cursor.GetKeySpan(ref buffer);
            PackUnpack.PackInt32LE(tempBuf, 0, key.Length);
            stream.Write(tempBuf, 0, 4);
            stream.Write(key);
            var value = cursor.GetValueSpan(ref buffer);
            PackUnpack.PackInt32LE(tempBuf, 0, value.Length);
            stream.Write(tempBuf, 0, 4);
            stream.Write(value);
            cursor.FindNextKey(new());
        }

        var ulongCount = transaction.GetUlongCount();
        if (transaction.GetCommitUlong() != 0 || ulongCount != 0)
        {
            PackUnpack.PackUInt64LE(tempBuf, 0, transaction.GetCommitUlong());
            stream.Write(tempBuf, 0, 8);
        }

        if (ulongCount != 0)
        {
            PackUnpack.PackUInt32LE(tempBuf, 0, ulongCount);
            stream.Write(tempBuf, 0, 4);
            for (var i = 0u; i < ulongCount; i++)
            {
                PackUnpack.PackUInt64LE(tempBuf, 0, transaction.GetUlong(i));
                stream.Write(tempBuf, 0, 8);
            }
        }
    }

    /// <summary>
    /// Reads and inserts all key value pairs from stream
    /// </summary>
    /// <param name="transaction">transaction where to import all data</param>
    /// <param name="stream">where to read it from</param>
    public static async Task ImportAsync(IKeyValueDBTransaction transaction, Stream stream)
    {
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));
        if (stream == null) throw new ArgumentNullException(nameof(stream));
        if (!stream.CanRead) throw new ArgumentException("stream must be readable", nameof(stream));
        var tempBuf = new byte[4096];
        var tempBuf2 = new byte[4096];
        if (await stream.ReadAsync(tempBuf.AsMemory(0, 16)) != 16) throw new EndOfStreamException();
        if (!tempBuf.AsSpan()[..8].SequenceEqual("BTDBEXP2"u8))
        {
            throw new BTDBException("Invalid header (it should Start with BTDBEXP2)");
        }

        using var cursor = transaction.CreateCursor();
        var keyValuePairs = PackUnpack.UnpackInt64LE(tempBuf, 8);
        if (keyValuePairs < 0) throw new BTDBException("Negative number of key value pairs");
        for (var kv = 0; kv < keyValuePairs; kv++)
        {
            if (await stream.ReadAsync(tempBuf.AsMemory(0, 4)) != 4) throw new EndOfStreamException();
            var keySize = PackUnpack.UnpackInt32LE(tempBuf, 0);
            if (keySize < 0) throw new BTDBException("Negative key size");
            if (keySize > tempBuf.Length) tempBuf = new byte[keySize];
            if (await stream.ReadAsync(tempBuf.AsMemory(0, keySize)) != keySize) throw new EndOfStreamException();
            if (await stream.ReadAsync(tempBuf2.AsMemory(0, 4)) != 4) throw new EndOfStreamException();
            var valueSize = PackUnpack.UnpackInt32LE(tempBuf2, 0);
            if (valueSize < 0) throw new BTDBException("Negative value size");
            if (valueSize > tempBuf2.Length) tempBuf2 = new byte[valueSize];
            if (await stream.ReadAsync(tempBuf2.AsMemory(0, valueSize)) != valueSize) throw new EndOfStreamException();
            cursor.CreateOrUpdateKeyValue(tempBuf.AsSpan(0, keySize), tempBuf2.AsSpan(0, valueSize));
        }

        if (await stream.ReadAsync(tempBuf.AsMemory(0, 8)) == 8)
        {
            transaction.SetCommitUlong(PackUnpack.UnpackUInt64LE(tempBuf, 0));
            if (await stream.ReadAsync(tempBuf.AsMemory(0, 4)) == 4)
            {
                var ulongCount = PackUnpack.UnpackUInt32LE(tempBuf, 0);
                for (var i = 0u; i < ulongCount; i++)
                {
                    if (await stream.ReadAsync(tempBuf.AsMemory(0, 8)) != 8) throw new EndOfStreamException();
                    transaction.SetUlong(i, PackUnpack.UnpackUInt64LE(tempBuf, 0));
                }
            }
        }
    }

    /// <summary>
    /// Reads and inserts all key value pairs from stream
    /// </summary>
    /// <param name="transaction">transaction where to import all data</param>
    /// <param name="stream">where to read it from</param>
    public static void Import(IKeyValueDBTransaction transaction, Stream stream)
    {
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));
        if (stream == null) throw new ArgumentNullException(nameof(stream));
        if (!stream.CanRead) throw new ArgumentException("stream must be readable", nameof(stream));
        var tempBuf = new byte[4096];
        var tempBuf2 = new byte[4096];
        if (stream.Read(tempBuf, 0, 16) != 16) throw new EndOfStreamException();
        if (!tempBuf.AsSpan()[..8].SequenceEqual("BTDBEXP2"u8))
        {
            throw new BTDBException("Invalid header (it should Start with BTDBEXP2)");
        }

        using var cursor = transaction.CreateCursor();
        var keyValuePairs = PackUnpack.UnpackInt64LE(tempBuf, 8);
        if (keyValuePairs < 0) throw new BTDBException("Negative number of key value pairs");
        for (var kv = 0; kv < keyValuePairs; kv++)
        {
            if (stream.Read(tempBuf, 0, 4) != 4) throw new EndOfStreamException();
            var keySize = PackUnpack.UnpackInt32LE(tempBuf, 0);
            if (keySize < 0) throw new BTDBException("Negative key size");
            if (keySize > tempBuf.Length) tempBuf = new byte[keySize];
            if (stream.Read(tempBuf, 0, keySize) != keySize) throw new EndOfStreamException();
            if (stream.Read(tempBuf2, 0, 4) != 4) throw new EndOfStreamException();
            var valueSize = PackUnpack.UnpackInt32LE(tempBuf2, 0);
            if (valueSize < 0) throw new BTDBException("Negative value size");
            if (valueSize > tempBuf2.Length) tempBuf2 = new byte[valueSize];
            if (stream.Read(tempBuf2, 0, valueSize) != valueSize) throw new EndOfStreamException();
            cursor.CreateOrUpdateKeyValue(tempBuf.AsSpan(0, keySize), tempBuf2.AsSpan(0, valueSize));
        }

        if (stream.Read(tempBuf, 0, 8) == 8)
        {
            transaction.SetCommitUlong(PackUnpack.UnpackUInt64LE(tempBuf, 0));
            if (stream.Read(tempBuf, 0, 4) == 4)
            {
                var ulongCount = PackUnpack.UnpackUInt32LE(tempBuf, 0);
                for (var i = 0u; i < ulongCount; i++)
                {
                    if (stream.Read(tempBuf, 0, 8) != 8) throw new EndOfStreamException();
                    transaction.SetUlong(i, PackUnpack.UnpackUInt64LE(tempBuf, 0));
                }
            }
        }
    }
}
