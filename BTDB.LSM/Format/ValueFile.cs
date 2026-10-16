using System;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.LSM.Format;

/// Location of a separated value: value file number, offset and length of the raw value.
readonly record struct ValueHandle(ulong FileNumber, long Offset, int Length)
{
    public byte[] Encode()
    {
        var encoder = new LsmEncoder(24);
        encoder.WriteVUInt64(FileNumber);
        encoder.WriteVUInt64((ulong)Offset);
        encoder.WriteVUInt64((ulong)Length);
        return encoder.WrittenSpan.ToArray();
    }

    public static ValueHandle Decode(ReadOnlySpan<byte> data)
    {
        var decoder = new LsmDecoder(data);
        return new(decoder.ReadVUInt64(), (long)decoder.ReadVUInt64(), decoder.ReadVUInt32());
    }

    /// Bytes the value occupies in its file, including its checksum.
    public long StoredLength => Length + 8L;
}

/// Writes value files (ImplementationPlan.md, section 10): each record is the raw value followed by its XxHash64.
/// A file is handed to fileComplete when it reaches the target size or the writer finishes; the caller creates it
/// before publishing any table that references it.
sealed class ValueFileWriter(Func<ulong> allocateFileNumber, long targetFileSize, Action<ulong, byte[]> fileComplete)
{
    ulong _fileNumber;
    LsmEncoder? _content;

    public ValueHandle Add(ReadOnlySpan<byte> value)
    {
        if (_content != null && _content.Length >= targetFileSize) Finish();
        if (_content == null)
        {
            _fileNumber = allocateFileNumber();
            _content = new(64 * 1024);
        }

        var offset = _content.Length;
        _content.WriteBytes(value);
        _content.WriteUInt64(XxHash64.Hash(value));
        return new(_fileNumber, offset, value.Length);
    }

    public void Finish()
    {
        if (_content == null) return;
        fileComplete(_fileNumber, _content.WrittenSpan.ToArray());
        _content = null;
    }

    public static byte[] VerifyRecord(ReadOnlySpan<byte> record, string objectName)
    {
        var value = record[..^8];
        if (XxHash64.Hash(value) != BitConverter.ToUInt64(record[^8..]))
            throw new BTDBException($"Value in {objectName} has a checksum mismatch.");
        return value.ToArray();
    }
}
