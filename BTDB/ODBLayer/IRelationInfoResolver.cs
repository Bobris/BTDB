using BTDB.FieldHandler;

namespace BTDB.ODBLayer
{
    public interface IRelationInfoResolver
    {
        IFieldHandlerFactory FieldHandlerFactory { get; }
        ITypeConvertorGenerator TypeConvertorGenerator { get; }
    }
}