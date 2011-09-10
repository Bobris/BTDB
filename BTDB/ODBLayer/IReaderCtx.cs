using BTDB.KVDBLayer.ReaderWriters;

namespace BTDB.ODBLayer
{
    public interface IReaderCtx
    {
        object ReadObject();
        void SkipObject();
        AbstractBufferedReader Reader();
    }
}