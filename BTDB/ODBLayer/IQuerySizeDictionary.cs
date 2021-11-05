using System.Collections.Generic;

namespace BTDB.ODBLayer;

public interface IQuerySizeDictionary<in TKey>
{
    IEnumerable<KeyValuePair<uint, uint>> QuerySizeEnumerator();
    KeyValuePair<uint, uint> QuerySizeByKey(TKey key);
}
