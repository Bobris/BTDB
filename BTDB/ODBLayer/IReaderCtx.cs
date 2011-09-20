using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public interface IReaderCtx
    {
        object ReadObject();
        void SkipObject();
        AbstractBufferedReader Reader();
    }
}