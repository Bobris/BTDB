using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BTDB.Collections;
using Xunit;

namespace BTDBTest;

// Builds Utf16Utf8Dictionary from random distinct keys, non-ASCII and empty ones included, and checks that lookups by
// UTF-16 and by UTF-8 agree with a Dictionary for present and absent keys.
public class Utf16Utf8DictionaryDifferentialTest
{
    static readonly string[] Pieces = ["a", "b", "ž", "ř", "😀", "日本", "ÿ", "xĀ", ""];

    static string RandomKey(Random random)
    {
        var sb = new StringBuilder();
        for (var i = random.Next(4); i > 0; i--) sb.Append(Pieces[random.Next(Pieces.Length)]);
        return sb.ToString();
    }

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 40).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void LookupsByUtf16AndUtf8Agree(int seed)
    {
        var random = new Random(seed);
        var keys = new HashSet<string>();
        for (var i = random.Next(0, 60); i > 0; i--) keys.Add(RandomKey(random));
        var list = keys.ToList();
        var dictionary = new Utf16Utf8Dictionary<int>(list.Count, list);
        for (var i = 0; i < list.Count; i++) dictionary.ValueRef((uint)i) = i * 10;

        for (var i = 0; i < list.Count; i++)
        {
            var key = list[i];
            var utf8 = Encoding.UTF8.GetBytes(key);
            Assert.Equal(i, dictionary.GetIndex(key.AsSpan()));
            Assert.Equal(i, dictionary.GetIndex(utf8));
            Assert.True(dictionary.TryGetValue(key.AsSpan(), out var v16));
            Assert.True(dictionary.TryGetValue(utf8, out var v8));
            Assert.Equal(i * 10, v16);
            Assert.Equal(i * 10, v8);
            Assert.Equal(key, dictionary.KeyUtf16((uint)i).ToString());
            Assert.Equal(utf8, dictionary.KeyUtf8((uint)i).ToArray());
        }

        for (var probe = 0; probe < 100; probe++)
        {
            var key = RandomKey(random);
            var expected = keys.Contains(key);
            Assert.Equal(expected, dictionary.ContainsKey(key.AsSpan()));
            Assert.Equal(expected, dictionary.ContainsKey(Encoding.UTF8.GetBytes(key)));
        }

        Assert.Equal(Enumerable.Range(0, list.Count).Select(i => (uint)i), dictionary.Index.ToList());
    }
}
