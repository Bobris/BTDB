using System.IO;

namespace BTDB.KVDBLayer;

public class JustDeleteFileCollectionStrategy : IDeleteFileCollectionStrategy
{
    public void DeleteFile(string fileName)
    {
        File.Delete(fileName);
    }
}
