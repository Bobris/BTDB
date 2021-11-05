namespace BTDB.IL;

public interface IILMethod
{
    void ExpectedLength(int length);
    bool InitLocals { get; set; }
    IILGen Generator { get; }
}
