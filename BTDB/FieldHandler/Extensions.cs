using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public static class Extensions
    {
        static public void WriteFieldHandler(this AbstractBufferedWriter writer, IFieldHandler handler)
        {
            writer.WriteString(handler.Name);
            writer.WriteByteArray(handler.Configuration);
        }

        static public IFieldHandler CreateFromReader(this IFieldHandlerFactory factory,AbstractBufferedReader reader)
        {
            var handlerName = reader.ReadString();
            var handlerConfiguration = reader.ReadByteArray();
            return factory.CreateFromName(handlerName, handlerConfiguration);
        }
    }
}