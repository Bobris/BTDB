using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public interface IWriterCtx
    {
        // Return true if actual content needs to be serialized
        bool WriteObject(object @object);
        AbstractBufferedWriter Writer();
    }
}