namespace BTDB.ODBLayer;

// Just marker interface to mark object as lazy loaded, which could be problem if done in different thread or after transaction is closed
public interface IAmLazyDBObject
{
}
