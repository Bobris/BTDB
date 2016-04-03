namespace BTDB.ODBLayer
{
    public interface IRelationCreator<T>
    {
        T Create(IObjectDBTransaction tr);
    }
}