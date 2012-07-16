namespace BTDB.KV2DBLayer
{
    public interface IHaveSubDB
    {
        T GetSubDB<T>(long id) where T : class;
    }
}