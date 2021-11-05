using System;
using System.Collections.Generic;

namespace DBBenchmark;

public interface IDbTimeTests : IDisposable
{
    (TimeSpan openTime, long memorySize) Open();

    TimeSpan Insert(byte[] key, byte[] value);

    /// <summary>
    /// Commit data after inserting all data do DB 
    /// </summary>
    TimeSpan InsertRange(Dictionary<byte[], byte[]> data);
    TimeSpan InsertRangeCommitByItem(Dictionary<byte[], byte[]> data);

    TimeSpan Read(byte[] key);
    TimeSpan ReadValues(IEnumerable<byte[]> keys);
    TimeSpan ReadAll(Dictionary<byte[], byte[]> exceptedData);

    TimeSpan Delete(byte[] key);
    TimeSpan DeleteAll();
}
