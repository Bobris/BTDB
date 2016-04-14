using BTDB.FieldHandler;

namespace BTDB.ODBLayer
{
    interface IRelationInfoResolver
    {
        IFieldHandlerFactory FieldHandlerFactory { get; }
        ITypeConvertorGenerator TypeConvertorGenerator { get; }
    }
}