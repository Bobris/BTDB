using System;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

class FileTransactionLog : IFileInfo, IFileTransactionLog
{
    readonly Guid? _guid;
    readonly uint _previousFileId;
    readonly long _generation;

    public FileTransactionLog(ref SpanReader reader, Guid? guid)
    {
        _guid = guid;
        _generation = reader.ReadVInt64();
        _previousFileId = (uint)reader.ReadVInt32();
    }

    public FileTransactionLog(long generation, Guid? guid, uint fileIdWithPreviousTransactionLog)
    {
        _guid = guid;
        _generation = generation;
        _previousFileId = fileIdWithPreviousTransactionLog;
    }

    public KVFileType FileType => KVFileType.TransactionLog;

    public Guid? Guid => _guid;

    public long Generation => _generation;

    public long SubDBId => 0;

    public uint PreviousFileId => _previousFileId;

    public uint NextFileId { get; set; }

    internal static void SkipHeader(ref SpanReader reader)
    {
        FileCollectionWithFileInfos.SkipHeader(ref reader);
        reader.SkipUInt8(); // type of file
        reader.SkipVInt64(); // generation
        reader.SkipVInt32(); // previous file id
    }

    internal void WriteHeader(ref SpanWriter writer)
    {
        FileCollectionWithFileInfos.WriteHeader(ref writer, _guid);
        writer.WriteUInt8((byte)KVFileType.TransactionLog);
        writer.WriteVInt64(_generation);
        writer.WriteVInt32((int)_previousFileId);
    }
}
