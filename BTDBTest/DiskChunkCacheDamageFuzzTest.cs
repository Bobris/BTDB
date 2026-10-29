using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using BTDB.Buffer;
using BTDB.ChunkCache;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

/// A cache with random bytes of one of its files changed, inserted, removed or cut off opens, and returns stored content
/// or nothing for each key, never other content.
public class DiskChunkCacheDamageFuzzTest
{
    public static TheoryData<int> Seeds() => new(Enumerable.Range(1, 60));

    // Before the fix values had no checksum, so damage to a value file or to the hash index made Get return other
    // content for about one key in seven.
    [Theory]
    [MemberData(nameof(Seeds))]
    public void DamagedCacheReturnsStoredContentOrNothing(int seed)
    {
        var random = new Random(seed);
        var directory = Path.Combine(Path.GetTempPath(), "btdb-cache-fuzz-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var keys = new ByteBuffer[40];
            using (var files = new OnDiskFileCollection(directory))
            using (var cache = new DiskChunkCache(files, 20, 1_000_000))
            {
                for (var i = 0; i < keys.Length; i++)
                {
                    var content = new byte[random.Next(1, random.Next(3) == 0 ? 5000 : 50)];
                    random.NextBytes(content);
                    keys[i] = ByteBuffer.NewAsync(SHA1.HashData(content));
                    cache.Put(keys[i], ByteBuffer.NewAsync(content));
                }
            }

            var originals = Directory.GetFiles(directory).ToDictionary(f => f, File.ReadAllBytes);
            for (var round = 0; round < 20; round++)
            {
                var victim = originals.Keys.ElementAt(random.Next(originals.Count));
                var bytes = originals[victim].ToList();
                for (var changes = random.Next(1, 4); changes > 0 && bytes.Count > 0; changes--)
                {
                    var at = random.Next(bytes.Count);
                    switch (random.Next(4))
                    {
                        case 0: bytes.RemoveRange(at, bytes.Count - at); break;
                        case 1: bytes.Insert(at, (byte)random.Next(256)); break;
                        case 2: bytes.RemoveAt(at); break;
                        default: bytes[at] ^= (byte)(1 << random.Next(8)); break;
                    }
                }

                File.WriteAllBytes(victim, bytes.ToArray());
                try
                {
                    using var files = new OnDiskFileCollection(directory);
                    using var cache = new DiskChunkCache(files, 20, 1_000_000);
                    foreach (var key in keys)
                    {
                        var got = cache.Get(key).Result;
                        Assert.True(got.Length == 0 ||
                                    SHA1.HashData(got.AsSyncReadOnlySpan()).AsSpan().SequenceEqual(key.AsSyncReadOnlySpan()),
                            $"Seed {seed} round {round} damaging {Path.GetFileName(victim)}: other content returned");
                    }
                }
                catch (Exception error) when (error is not Xunit.Sdk.XunitException)
                {
                    Assert.Fail($"Seed {seed} round {round} damaging {Path.GetFileName(victim)}: {error.GetType().Name}: {error.Message}\n{error.StackTrace}");
                }

                foreach (var file in Directory.GetFiles(directory)) File.Delete(file);
                foreach (var (path, content) in originals) File.WriteAllBytes(path, content);
            }
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }
}
