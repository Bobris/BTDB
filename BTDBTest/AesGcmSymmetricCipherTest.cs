using System.Linq;
using BTDB.Encrypted;
using Xunit;

namespace BTDBTest;

public class AesGcmSymmetricCipherTest
{
    [Fact]
    public void OrderedDecryptRestoresEveryValue()
    {
        var cipher = new AesGcmSymmetricCipher(Enumerable.Range(0, AesGcmSymmetricCipher.KeySize)
            .Select(i => (byte)i).ToArray());
        var plains = new[]
        {
            Enumerable.Repeat((byte)1, 32).ToArray(), Enumerable.Repeat((byte)7, 17).ToArray(), new byte[] { 3 },
            Enumerable.Repeat((byte)9, 48).ToArray()
        };
        var encrypted = plains.Select(plain =>
        {
            var output = new byte[cipher.CalcOrderedEncryptedSizeFor(plain)];
            cipher.OrderedEncrypt(plain, output);
            return output;
        }).ToArray();
        for (var round = 0; round < 2; round++)
        {
            for (var i = 0; i < plains.Length; i++)
            {
                var output = new byte[cipher.CalcOrderedPlainSizeFor(encrypted[i])];
                Assert.True(cipher.OrderedDecrypt(encrypted[i], output));
                Assert.Equal(plains[i], output[..plains[i].Length]);
                Assert.All(output[plains[i].Length..], b => Assert.Equal(0, b));
            }
        }
    }

    [Fact]
    public void OrderedEncryptionIsDeterministic()
    {
        var cipher = new AesGcmSymmetricCipher(new byte[AesGcmSymmetricCipher.KeySize]);
        var first = new byte[16];
        var second = new byte[16];
        cipher.OrderedEncrypt(new byte[] { 1, 2, 3 }, first);
        cipher.OrderedEncrypt(new byte[] { 1, 2, 3 }, second);
        Assert.Equal(first, second);
    }

    // Before the fix a length that is no whole number of blocks threw CryptographicException instead of failing.
    [Fact]
    public void OrderedDecryptOfPartialBlockFails()
    {
        var cipher = new AesGcmSymmetricCipher(new byte[AesGcmSymmetricCipher.KeySize]);
        var damaged = new byte[17];
        Assert.False(cipher.OrderedDecrypt(damaged, new byte[cipher.CalcOrderedPlainSizeFor(damaged)]));
    }
}
