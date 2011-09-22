using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public interface IWriterCtx
    {
        void WriteObject(object @object);
        AbstractBufferedWriter Writer();
    }
}