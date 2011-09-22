using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public interface IWriterCtx
    {
        void WriteObject(object @object);
        AbstractBufferedWriter Writer();
    }
}