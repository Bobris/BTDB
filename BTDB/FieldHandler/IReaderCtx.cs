using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public interface IReaderCtx
    {
        object ReadObject();
        void SkipObject();
        AbstractBufferedReader Reader();
    }
}