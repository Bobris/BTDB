using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BTDB.Replication;

/// <summary>
/// Versioned objects with metadata below one directory: the local-disk stand-in for an object storage container.
/// The metadata file <c>meta/{key}</c> exists exactly while the object exists and names its content file in
/// <c>data/</c>, so replacing the metadata file changes an object atomically. A whole-content write installs a fresh
/// content file; an append extends the current one beyond its committed length, which readers never pass. Versions
/// are random and never repeat, even after a delete.
/// One process owns the directory through an exclusive lock file; instances in that process share one lock and the
/// lease state of <see cref="FileSystemLeaderStorage"/>. Call every member except <see cref="Stage"/> under
/// <see cref="Lock"/>.
/// </summary>
internal sealed class FileSystemObjectStore : IDisposable
{
    internal sealed record Metadata(string Version, string DataFile, long Length, string? Sha256 = null,
        DateTimeOffset? DeleteAfter = null, string? OwnerSession = null, string? OwnerEndpoint = null);

    sealed class Shared(string root, FileStream lockFile)
    {
        public readonly object Lock = new();
        public readonly string Root = root;
        public readonly FileStream LockFile = lockFile;
        public int References;
        public string? LeaseHandle;
        public long LeaseExpiry;
    }

    static readonly Dictionary<string, Shared> Opened = new(StringComparer.Ordinal);
    const byte Format = 1;
    const int CopyBufferSize = 1024 * 1024;
    readonly Shared _shared;
    readonly string _meta, _data, _tmp;
    bool _disposed;

    /// <param name="directory">The store's own directory; created when missing.</param>
    /// <param name="flushToDisk">False skips the physical disk flush (fsync) of every write. Writes still reach the
    /// operating system, so they survive a crash of the process but not of the machine. Only for development and tests.</param>
    public FileSystemObjectStore(string directory, bool flushToDisk = true)
    {
        FlushToDisk = flushToDisk;
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        var root = Path.GetFullPath(directory);
        _meta = Path.Combine(root, "meta");
        _data = Path.Combine(root, "data");
        _tmp = Path.Combine(root, "tmp");
        lock (Opened)
        {
            if (!Opened.TryGetValue(root, out var shared))
            {
                Directory.CreateDirectory(root);
                FileStream lockFile;
                try
                {
                    lockFile = new(Path.Combine(root, ".lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite,
                        FileShare.None);
                }
                catch (IOException error)
                {
                    throw new IOException(
                        $"Directory '{root}' is used by another process; a file-system store serves one process.", error);
                }
                try { Recover(); }
                catch
                {
                    lockFile.Dispose();
                    throw;
                }
                shared = new(root, lockFile);
                Opened.Add(root, shared);
            }
            shared.References++;
            _shared = shared;
        }
    }

    public string Root => _shared.Root;

    public bool FlushToDisk { get; }

    /// <summary>Flush to the operating system and, unless disabled, to the disk.</summary>
    public void Flush(FileStream stream) => stream.Flush(FlushToDisk);

    public object Lock
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _shared.Lock;
        }
    }

    public string? LeaseHandle { get => _shared.LeaseHandle; set => _shared.LeaseHandle = value; }
    public long LeaseExpiry { get => _shared.LeaseExpiry; set => _shared.LeaseExpiry = value; }

    // Only this process uses the directory: unfinished staging and content files no metadata names are crash leftovers.
    void Recover()
    {
        if (Directory.Exists(_tmp)) Directory.Delete(_tmp, true);
        Directory.CreateDirectory(_tmp);
        Directory.CreateDirectory(_data);
        Directory.CreateDirectory(_meta);
        var referenced = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in Directory.EnumerateFiles(_meta, "*", SearchOption.AllDirectories))
            referenced.Add(Decode(File.ReadAllBytes(file), file).DataFile);
        foreach (var file in Directory.EnumerateFiles(_data))
            if (!referenced.Contains(Path.GetFileName(file))) File.Delete(file);
    }

    /// <summary>Keys use [a-z0-9-_./] (the directory may be case-insensitive) and map to relative paths.</summary>
    public static void ValidateKey(string key)
    {
        if (string.IsNullOrEmpty(key) || key.Length > 1024) throw new FormatException("Invalid object key.");
        foreach (var c in key)
            if (!(c is >= 'a' and <= 'z' or >= '0' and <= '9' or '/' or '-' or '_' or '.'))
                throw new FormatException("File-system object keys use only [a-z0-9-_./].");
        foreach (var segment in key.Split('/'))
            if (segment is "" or "." or "..") throw new FormatException("Invalid object key.");
    }

    string MetaPath(string key)
    {
        ValidateKey(key);
        return Path.Combine(_meta, key.Replace('/', Path.DirectorySeparatorChar));
    }

    string DataPath(string dataFile) => Path.Combine(_data, dataFile);

    public static string NewVersion() => Guid.NewGuid().ToString("N");

    public Metadata? Get(string key)
    {
        var path = MetaPath(key);
        return File.Exists(path) ? Decode(File.ReadAllBytes(path), path) : null;
    }

    /// <summary>Atomically install or replace the object's metadata, and so the object.</summary>
    public void Put(string key, Metadata metadata)
    {
        var path = MetaPath(key);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var staged = Stage();
        try
        {
            using (var stream = new FileStream(staged, FileMode.CreateNew, FileAccess.Write))
            {
                stream.Write(Encode(metadata));
                Flush(stream);
            }
            File.Move(staged, path, true);
        }
        finally { File.Delete(staged); }
    }

    /// <summary>Delete the object, then its content file.</summary>
    public void Remove(string key, Metadata metadata)
    {
        File.Delete(MetaPath(key));
        DeleteData(metadata.DataFile);
    }

    public void DeleteData(string dataFile) => File.Delete(DataPath(dataFile));

    /// <summary>The content file's write time stands for the object's last content write.</summary>
    public DateTimeOffset GetModified(string dataFile) => File.GetLastWriteTimeUtc(DataPath(dataFile));

    public void SetModified(string dataFile, DateTimeOffset time) => File.SetLastWriteTimeUtc(DataPath(dataFile), time.UtcDateTime);

    /// <summary>A fresh content file holding exactly <paramref name="content"/>.</summary>
    public string WriteData(ReadOnlySpan<byte> content)
    {
        var name = NewVersion();
        using var stream = new FileStream(DataPath(name), FileMode.CreateNew, FileAccess.Write);
        stream.Write(content);
        Flush(stream);
        return name;
    }

    /// <summary>Write <paramref name="count"/> bytes from <paramref name="source"/> at <paramref name="offset"/> of a
    /// content file, created when null, dropping bytes beyond the committed offset left by an interrupted append.</summary>
    public string Append(string? dataFile, long offset, long count, Action<long, Span<byte>> source)
    {
        dataFile ??= NewVersion();
        using var stream = new FileStream(DataPath(dataFile), FileMode.OpenOrCreate, FileAccess.ReadWrite);
        if (stream.Length < offset) throw new InvalidDataException("A content file is shorter than its metadata.");
        stream.SetLength(offset);
        stream.Position = offset;
        var buffer = new byte[(int)Math.Min(count, CopyBufferSize)];
        for (long done = 0; done < count;)
        {
            var chunk = buffer.AsSpan(0, (int)Math.Min(count - done, buffer.Length));
            source(done, chunk);
            stream.Write(chunk);
            done += chunk.Length;
        }
        Flush(stream);
        return dataFile;
    }

    /// <summary>A unique path for writing content outside the lock; install it with <see cref="Install"/>.</summary>
    public string Stage() => Path.Combine(_tmp, NewVersion());

    public string Install(string staged)
    {
        var name = NewVersion();
        File.Move(staged, DataPath(name));
        return name;
    }

    public void Read(Metadata metadata, long offset, Span<byte> destination)
    {
        if (offset < 0 || offset + destination.Length > metadata.Length)
            throw new ArgumentOutOfRangeException(nameof(offset));
        using var stream = new FileStream(DataPath(metadata.DataFile), FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        stream.Position = offset;
        stream.ReadExactly(destination);
    }

    public byte[] ReadAll(Metadata metadata)
    {
        var content = new byte[metadata.Length];
        Read(metadata, 0, content);
        return content;
    }

    /// <summary>Objects whose key starts with <paramref name="prefix"/>, in ordinal key order.</summary>
    public List<(string Key, Metadata Metadata)> List(string prefix)
    {
        var slash = prefix.LastIndexOf('/');
        var directory = slash < 0 ? "" : prefix[..(slash + 1)];
        var path = directory.Length == 0 ? _meta : MetaPath(directory.TrimEnd('/'));
        var result = new List<(string, Metadata)>();
        if (!Directory.Exists(path)) return result;
        foreach (var file in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
        {
            var key = Path.GetRelativePath(_meta, file).Replace(Path.DirectorySeparatorChar, '/');
            if (key.StartsWith(prefix, StringComparison.Ordinal))
                result.Add((key, Decode(File.ReadAllBytes(file), file)));
        }
        result.Sort((a, b) => string.CompareOrdinal(a.Item1, b.Item1));
        return result;
    }

    static byte[] Encode(Metadata metadata)
    {
        using var memory = new MemoryStream();
        using (var writer = new BinaryWriter(memory, Encoding.UTF8, true))
        {
            writer.Write(Format);
            writer.Write(metadata.Version);
            writer.Write(metadata.DataFile);
            writer.Write(metadata.Length);
            WriteOptional(writer, metadata.Sha256);
            writer.Write(metadata.DeleteAfter.HasValue);
            if (metadata.DeleteAfter is { } deadline) writer.Write(deadline.UtcTicks);
            WriteOptional(writer, metadata.OwnerSession);
            WriteOptional(writer, metadata.OwnerEndpoint);
        }
        return memory.ToArray();
    }

    static void WriteOptional(BinaryWriter writer, string? value)
    {
        writer.Write(value != null);
        if (value != null) writer.Write(value);
    }

    static Metadata Decode(byte[] bytes, string path)
    {
        try
        {
            using var reader = new BinaryReader(new MemoryStream(bytes), Encoding.UTF8);
            if (reader.ReadByte() != Format) throw new InvalidDataException("Unknown object metadata format.");
            var metadata = new Metadata(reader.ReadString(), reader.ReadString(), reader.ReadInt64(),
                ReadOptional(reader), reader.ReadBoolean() ? new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero) : null,
                ReadOptional(reader), ReadOptional(reader));
            if (reader.BaseStream.Position != bytes.Length || metadata.Length < 0 ||
                metadata.DataFile.Length == 0 || metadata.DataFile.IndexOfAny(['/', '\\', '.']) >= 0)
                throw new InvalidDataException("Invalid object metadata.");
            return metadata;
        }
        catch (Exception error) when (error is EndOfStreamException or InvalidDataException)
        {
            throw new InvalidDataException($"Object metadata '{path}' is invalid.", error);
        }
    }

    static string? ReadOptional(BinaryReader reader) => reader.ReadBoolean() ? reader.ReadString() : null;

    public void Dispose()
    {
        lock (Opened)
        {
            if (_disposed) return;
            _disposed = true;
            if (--_shared.References != 0) return;
            Opened.Remove(_shared.Root);
            _shared.LockFile.Dispose();
        }
    }
}
