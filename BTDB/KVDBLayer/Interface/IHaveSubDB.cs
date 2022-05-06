namespace BTDB.KVDBLayer;

public interface IHaveSubDB
{
    T GetSubDB<T>(long id) where T : class;
}
