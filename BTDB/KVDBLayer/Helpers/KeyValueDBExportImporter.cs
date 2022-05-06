using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;

namespace BTDB.KVDBLayer;

public static class KeyValueDBExportImporter
{
    /// <summary>
    /// Writes all key value pairs in current prefix to stream (prefix itself is not written)
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
        var tempbuf = new byte[16];
        tempbuf[0] = (byte)'B';
        tempbuf[1] = (byte)'T';
        tempbuf[2] = (byte)'D';
        tempbuf[3] = (byte)'B';
        tempbuf[4] = (byte)'E';
        tempbuf[5] = (byte)'X';
        tempbuf[6] = (byte)'P';
        tempbuf[7] = (byte)'2';
        PackUnpack.PackInt64LE(tempbuf, 8, keyValueCount);
        stream.Write(tempbuf, 0, 16);
        transaction.FindFirstKey(new ReadOnlySpan<byte>());
        Span<byte> keyBuffer = stackalloc byte[512];
        for (long kv = 0; kv < keyValueCount; kv++)
        {
            var key = transaction.GetKey(ref MemoryMarshal.GetReference(keyBuffer), keyBuffer.Length);
            PackUnpack.PackInt32LE(tempbuf, 0, key.Length);
            stream.Write(tempbuf, 0, 4);
            stream.Write(key);
            var value = transaction.GetValue();
            PackUnpack.PackInt32LE(tempbuf, 0, value.Length);
            stream.Write(tempbuf, 0, 4);
            stream.Write(value);
            transaction.FindNextKey(new ReadOnlySpan<byte>());
        }
        var ulongCount = transaction.GetUlongCount();
        if (transaction.GetCommitUlong() != 0 || ulongCount != 0)
        {
            PackUnpack.PackUInt64LE(tempbuf, 0, transaction.GetCommitUlong());
            stream.Write(tempbuf, 0, 8);
        }
        if (ulongCount != 0)
        {
            PackUnpack.PackUInt32LE(tempbuf, 0, ulongCount);
            stream.Write(tempbuf, 0, 4);
            for (var i = 0u; i < ulongCount; i++)
            {
                PackUnpack.PackUInt64LE(tempbuf, 0, transaction.GetUlong(i));
                stream.Write(tempbuf, 0, 8);
            }
        }
    }

    /// <summary>
    /// Reads and inserts all key value pairs into current prefix from stream
    /// </summary>
    /// <param name="transaction">transaction where to import all data</param>
    /// <param name="stream">where to read it from</param>
    public static void Import(IKeyValueDBTransaction transaction, Stream stream)
    {
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));
        if (stream == null) throw new ArgumentNullException(nameof(stream));
        if (!stream.CanRead) throw new ArgumentException("stream must be readable", nameof(stream));
        var tempbuf = new byte[4096];
        var tempbuf2 = new byte[4096];
        if (stream.Read(tempbuf, 0, 16) != 16) throw new EndOfStreamException();
        if (tempbuf[0] != 'B' || tempbuf[1] != 'T' || tempbuf[2] != 'D' || tempbuf[3] != 'B' || tempbuf[4] != 'E' || tempbuf[5] != 'X' || tempbuf[6] != 'P' || tempbuf[7] != '2')
        {
            throw new BTDBException("Invalid header (it should Start with BTDBEXP2)");
        }
        var keyValuePairs = PackUnpack.UnpackInt64LE(tempbuf, 8);
        if (keyValuePairs < 0) throw new BTDBException("Negative number of key value pairs");
        for (var kv = 0; kv < keyValuePairs; kv++)
        {
            if (stream.Read(tempbuf, 0, 4) != 4) throw new EndOfStreamException();
            var keySize = PackUnpack.UnpackInt32LE(tempbuf, 0);
            if (keySize < 0) throw new BTDBException("Negative key size");
            if (keySize > tempbuf.Length) tempbuf = new byte[keySize];
            if (stream.Read(tempbuf, 0, keySize) != keySize) throw new EndOfStreamException();
            if (stream.Read(tempbuf2, 0, 4) != 4) throw new EndOfStreamException();
            var valueSize = PackUnpack.UnpackInt32LE(tempbuf2, 0);
            if (valueSize < 0) throw new BTDBException("Negative value size");
            if (valueSize > tempbuf2.Length) tempbuf2 = new byte[valueSize];
            if (stream.Read(tempbuf2, 0, valueSize) != valueSize) throw new EndOfStreamException();
            transaction.CreateOrUpdateKeyValue(tempbuf.AsSpan(0, keySize), tempbuf2.AsSpan(0, valueSize));
        }
        if (stream.Read(tempbuf, 0, 8) == 8)
        {
            transaction.SetCommitUlong(PackUnpack.UnpackUInt64LE(tempbuf, 0));
            if (stream.Read(tempbuf, 0, 4) == 4)
            {
                var ulongCount = PackUnpack.UnpackUInt32LE(tempbuf, 0);
                for (var i = 0u; i < ulongCount; i++)
                {
                    if (stream.Read(tempbuf, 0, 8) != 8) throw new EndOfStreamException();
                    transaction.SetUlong(i, PackUnpack.UnpackUInt64LE(tempbuf, 0));
                }
            }
        }
    }
}
