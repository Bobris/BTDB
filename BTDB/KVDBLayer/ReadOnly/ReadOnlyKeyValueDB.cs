using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer.ReadOnly;

/// File format written by <see cref="ReadOnlyCreator"/>. All offsets are from the start of the data and fit 32 bits.
/// <code>
/// Header: "roBTDB" | Int16BE version | VUInt64 key count | VUInt64 commit ulong | VUInt32 ulong count | VUInt64 ulongs
/// Value:  VUInt32 length | bytes (an empty value is stored as offset 0)
/// Leaf:   UInt8 key count (1..127) | VUInt32 prefix length | prefix | UInt16LE end of each key suffix | key suffixes
///         | UInt32LE value offset of each key
/// Branch: UInt8 128 + child count (1..127) | VUInt32 prefix length | prefix | UInt16LE end of each separator suffix
///         | separator suffixes | UInt32LE child offsets | UInt32LE key count in children up to and including this one
/// Footer: UInt32LE root node offset (0 for an empty database) | UInt64LE XxHash64 of all data before it (version 2)
/// </code>
/// Separator i of a branch is the first key of child i + 1, the prefix is common to the first keys of all children.
static unsafe class ReadOnlyFormat
{
    internal static ReadOnlySpan<byte> Magic => "roBTDB"u8;
    internal const short Version = 2;
    internal const short VersionWithoutChecksum = 1;
    internal const uint MaxChildren = 127;
    internal const uint DefaultMaxChildren = 31;
    internal const int MaxNodeKeyBytes = ushort.MaxValue;
    internal const byte BranchFlag = 128;

    internal static uint ReadUInt16LE(nuint address) =>
        BinaryPrimitives.ReadUInt16LittleEndian(new ReadOnlySpan<byte>((void*)address, 2));

    internal static uint ReadUInt32LE(nuint address) =>
        BinaryPrimitives.ReadUInt32LittleEndian(new ReadOnlySpan<byte>((void*)address, 4));

    internal static ReadOnlySpan<byte> Span(nuint address, uint length) => new((void*)address, (int)length);

    internal static uint ReadVUInt32(nuint address, out uint length)
    {
        ref var first = ref Unsafe.AsRef<byte>((void*)address);
        length = PackUnpack.LengthVUIntByFirstByte(first);
        if (length > 5) throw new InvalidDataException("Corrupted read only database");
        return (uint)PackUnpack.UnsafeUnpackVUInt(ref first, length);
    }
}

/// Immutable key value database stored in one continuous block of memory written by <see cref="ReadOnlyCreator"/>.
/// Usually it is a memory mapped file opened by <see cref="OpenFile"/>. All transactions see the same data and every
/// attempt to modify it throws <see cref="BTDBException"/>.
public sealed class ReadOnlyKeyValueDB : IKeyValueDB
{
    internal readonly nuint Begin;
    internal readonly uint TotalLength;
    // Where the footer starts, the end of the header, values and nodes.
    internal readonly uint FooterOffset;
    internal readonly uint RootOffset;
    internal readonly long KeyValueCount;
    internal readonly ulong CommitUlong;
    internal readonly ulong[] Ulongs;
    MemoryHandle _pinnedMemory;
    MemoryMappedFile? _mappedFile;
    MemoryMappedViewAccessor? _mappedView;
    bool _disposed;

    /// Opens data at the given address. The memory must stay valid and unchanged until this database is disposed.
    /// Only the header and footer are read; <paramref name="verifyChecksum"/> reads all data to compare its checksum.
    public unsafe ReadOnlyKeyValueDB(nuint begin, uint totalLength, bool verifyChecksum = false)
    {
        Begin = begin;
        TotalLength = totalLength;
        if (totalLength < ReadOnlyFormat.Magic.Length + 2 + 4)
            throw new InvalidDataException("Data are too short to be read only database");
        var reader = new MemReader((void*)begin, (int)Math.Min(totalLength - 4, int.MaxValue));
        if (!reader.CheckMagic(ReadOnlyFormat.Magic)) throw new InvalidDataException("Wrong header magic");
        var version = reader.ReadInt16BE();
        if (version is not (ReadOnlyFormat.Version or ReadOnlyFormat.VersionWithoutChecksum))
            throw new InvalidDataException("Unsupported version");
        HasChecksum = version == ReadOnlyFormat.Version;
        var footerLength = HasChecksum ? 12u : 4u;
        if (totalLength < ReadOnlyFormat.Magic.Length + 2 + footerLength)
            throw new InvalidDataException("Data are too short to be read only database");
        FooterOffset = totalLength - footerLength;
        if (verifyChecksum) VerifyChecksum();
        try
        {
            KeyValueCount = checked((long)reader.ReadVUInt64());
            CommitUlong = reader.ReadVUInt64();
            var ulongCount = reader.ReadVUInt32();
            if (ulongCount > totalLength) throw new InvalidDataException("Corrupted read only database header");
            Ulongs = new ulong[ulongCount];
            for (var i = 0u; i < ulongCount; i++)
            {
                Ulongs[i] = reader.ReadVUInt64();
            }
        }
        // A header running past the data or holding a count no long can, as damage leaves, is truncated data.
        catch (Exception error) when (error is EndOfStreamException or OverflowException)
        {
            throw new InvalidDataException("Corrupted read only database header", error);
        }

        RootOffset = ReadOnlyFormat.ReadUInt32LE(begin + FooterOffset);
        if (RootOffset >= FooterOffset || (RootOffset == 0) != (KeyValueCount == 0))
            throw new InvalidDataException("Corrupted read only database footer");
    }

    /// The data end with the XxHash64 of their content, as <see cref="ReadOnlyCreator"/> writes them since version 2.
    public bool HasChecksum { get; }

    /// Reads all data and throws <see cref="InvalidDataException"/> when they do not match their checksum, or have none.
    /// Without it damaged keys and values read as other content; damaged structure fails when reached.
    public unsafe void VerifyChecksum()
    {
        if (!HasChecksum) throw new InvalidDataException("Read only database has no checksum");
        var checksumOffset = TotalLength - 8;
        var stored = BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)(Begin + checksumOffset), 8));
        // Spans are limited to 2GB, so longer data are hashed in parts.
        var hash = new XxHash64();
        for (var offset = 0u; offset < checksumOffset;)
        {
            var length = Math.Min(checksumOffset - offset, 1u << 30);
            hash.Update(new ReadOnlySpan<byte>((void*)(Begin + offset), (int)length));
            offset += length;
        }

        if (hash.Digest() != stored) throw new InvalidDataException("Read only database checksum does not match");
    }

    /// Opens data in memory, the memory is pinned until this database is disposed and must not change.
    /// <paramref name="verifyChecksum"/> reads all data to compare their checksum first.
    public static unsafe ReadOnlyKeyValueDB Create(ReadOnlyMemory<byte> data, bool verifyChecksum = false)
    {
        var handle = data.Pin();
        try
        {
            return new((nuint)handle.Pointer, (uint)data.Length, verifyChecksum) { _pinnedMemory = handle };
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    /// Memory maps the file written by <see cref="ReadOnlyCreator.WriteToFile"/>. <paramref name="verifyChecksum"/>
    /// reads the whole file to compare its checksum first, otherwise pages are read as accessed.
    public static unsafe ReadOnlyKeyValueDB OpenFile(string fileName, bool verifyChecksum = false)
    {
        var length = new FileInfo(fileName).Length;
        if (length > uint.MaxValue) throw new InvalidDataException("Read only database cannot be bigger than 4GB");
        var mappedFile = MemoryMappedFile.CreateFromFile(fileName, FileMode.Open, null, 0,
            MemoryMappedFileAccess.Read);
        MemoryMappedViewAccessor? view = null;
        var acquired = false;
        try
        {
            view = mappedFile.CreateViewAccessor(0, length, MemoryMappedFileAccess.Read);
            byte* pointer = null;
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
            acquired = true;
            var begin = (nuint)(pointer + view.PointerOffset);
            return new(begin, (uint)length, verifyChecksum) { _mappedFile = mappedFile, _mappedView = view };
        }
        catch
        {
            if (acquired) view!.SafeMemoryMappedViewHandle.ReleasePointer();
            view?.Dispose();
            mappedFile.Dispose();
            throw;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _pinnedMemory.Dispose();
        if (_mappedView != null)
        {
            _mappedView.SafeMemoryMappedViewHandle.ReleasePointer();
            _mappedView.Dispose();
            _mappedFile!.Dispose();
        }
    }

    internal void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    internal static BTDBException ReadOnlyException() => new("Read only database cannot be modified");

    public bool DurableTransactions { get; set; }

    public IKeyValueDBTransaction StartTransaction() => StartReadOnlyTransaction();

    public IKeyValueDBTransaction StartReadOnlyTransaction()
    {
        ThrowIfDisposed();
        return new ReadOnlyKeyValueDBTransaction(this);
    }

    public ValueTask<IKeyValueDBTransaction> StartWritingTransaction(bool inBatch = false) =>
        ValueTask.FromException<IKeyValueDBTransaction>(ReadOnlyException());

    public string CalcStats() => $"Read only database with {KeyValueCount} keys in {TotalLength} bytes";

    public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats() =>
        (0, 0, 0, 0);

    public ValueTask<bool> Compact(CancellationToken cancellation) => ValueTask.FromResult(false);

    public void CreateKvi(CancellationToken cancellation) => throw ReadOnlyException();

    public ulong? PreserveHistoryUpToCommitUlong { get; set; }
    public IKeyValueDBLogger? Logger { get; set; }
    public uint CompactorRamLimitInMb { get; set; }
    public long MaxTrLogFileSize { get; set; }

    public IEnumerable<IKeyValueDBTransaction> Transactions() => [];

    public ulong CompactorReadBytesPerSecondLimit { get; set; }
    public ulong CompactorWriteBytesPerSecondLimit { get; set; }
}
