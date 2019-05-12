namespace BTDB.IL
{
    public interface IILMethod
    {
        void ExpectedLength(int length);
        IILGen Generator { get; }
    }
}