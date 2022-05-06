namespace BTDB.EventStoreLayer;

public interface IEventFileStorage
{
    uint MaxBlockSize { get; }
    ulong MaxFileSize { get; }
    uint Read(Buffer.ByteBuffer buf, ulong position);
    void Write(Buffer.ByteBuffer buf, ulong position);
    IEventFileStorage CreateNew(IEventFileStorage previous);
}
