namespace BTDB.EventStoreLayer
{
    public interface ITypeBinaryDeserializerContext
    {
        object LoadObject();
        void AddBackRef(object obj);
        void SkipObject();
        ITypeDescriptor CurrentDescriptor { get; }
    }
}