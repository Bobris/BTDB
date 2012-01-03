namespace BTDB.IL
{
    public interface IILDynamicMethodWithThis : IILMethod
    {
        object Create(object @this);
    }
}