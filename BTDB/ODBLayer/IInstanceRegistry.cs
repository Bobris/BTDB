namespace BTDB.ODBLayer
{
    public interface IInstanceRegistry
    {
        int RegisterInstance(object content);
        object FindInstance(int id);
    }
}