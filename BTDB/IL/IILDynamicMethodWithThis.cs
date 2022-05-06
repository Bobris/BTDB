namespace BTDB.IL;

public interface IILDynamicMethodWithThis : IILMethod
{
    void FinalizeCreation();
    object Create(object? @this);
}
