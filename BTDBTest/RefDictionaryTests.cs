using System.Collections.Generic;
using System.Linq;
using BTDB.Collections;
using Xunit;

namespace BTDBTest;

public class RefDictionaryTests
{
    [Fact]
    public void TryAddAndTryGetValue()
    {
        var d = new RefDictionary<string, int>();
        Assert.Empty(d);
        Assert.True(d.TryAdd("a", 1));
        Assert.False(d.TryAdd("a", 2));
        Assert.Single(d);
        Assert.True(d.TryGetValue("a", out var v));
        Assert.Equal(1, v);
    }

    [Fact]
    public void GetOrAddValueRef_setsValueByRef()
    {
        var d = new RefDictionary<string, int>();
        ref var r = ref d.GetOrAddValueRef("k");
        r = 123;
        Assert.True(d.TryGetValue("k", out var v));
        Assert.Equal(123, v);
    }

    [Fact]
    public void GetOrFakeValueRef_returnsFakeForMissingKeys()
    {
        var d = new RefDictionary<string, int>();
        ref var f1 = ref d.GetOrFakeValueRef("missing1");
        f1 = 555;
        Assert.False(d.TryGetValue("missing1", out _));
        ref var f2 = ref d.GetOrFakeValueRef("missing2");
        // fake storage is shared - writing to fake for missing1 affects fake observed for another missing key
        Assert.Equal(555, f2);
        d.GetOrFakeValueRef("missing3", out bool found);
        Assert.False(found);
    }

    [Fact]
    public void Remove_updatesCountAndRemovesValue()
    {
        var d = new RefDictionary<string, int>();
        d.TryAdd("a", 1);
        d.TryAdd("b", 2);
        d.TryAdd("c", 3);
        Assert.Equal(3, d.Count);
        Assert.True(d.Remove("b"));
        Assert.Equal(2, d.Count);
        Assert.False(d.TryGetValue("b", out _));
        Assert.False(d.Remove("non"));
    }

    [Fact]
    public void IndexEnumerator_and_KeyValueRefs_work()
    {
        var d = new RefDictionary<string, int>();
        d.TryAdd("one", 1);
        d.TryAdd("two", 2);
        d.TryAdd("three", 3);
        var indices = d.Index.ToArray();
        Assert.Equal(3, indices.Length);
        var seen = new HashSet<string>();
        foreach (var idx in indices)
        {
            var key = d.KeyRef(idx);
            var value = d.ValueRef(idx);
            seen.Add(key);
            Assert.True(d.TryGetValue(key, out var vv));
            Assert.Equal(vv, value);
        }

        Assert.Contains("one", seen);
        Assert.Contains("two", seen);
        Assert.Contains("three", seen);
    }

    [Fact]
    public void Clear_removesAllEntries_and_allowsReadd()
    {
        var d = new RefDictionary<string, int>();
        d.TryAdd("a", 1);
        d.TryAdd("b", 2);
        Assert.Equal(2, d.Count);
        d.Clear();
        Assert.Empty(d);
        Assert.False(d.TryGetValue("a", out _));
        Assert.False(d.TryGetValue("b", out _));
        Assert.Empty(d.Index);

        // After clear we can add again and behavior is normal
        d.TryAdd("c", 3);
        Assert.Single(d);
        Assert.True(d.TryGetValue("c", out var v));
        Assert.Equal(3, v);
    }
}
