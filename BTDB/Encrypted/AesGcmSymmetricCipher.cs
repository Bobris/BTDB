using System;
using System.Security.Cryptography;

namespace BTDB.Encrypted;

public class AesGcmSymmetricCipher : ISymmetricCipher
{
    public const int KeySize = 32;
    const int NonceSize = 12;
    const int TagSize = 16;
    readonly AesGcm _aes;
    readonly ICryptoTransform _decryptor;
    readonly ICryptoTransform _encryptor;
    readonly object _lock = new object();

    public AesGcmSymmetricCipher(byte[] key)
    {
        _aes = new AesGcm(key);
        var aes = Aes.Create();
        aes.Key = key;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.Zeros;
        aes.IV = new byte[16];
        _decryptor = aes.CreateDecryptor();
        _encryptor = aes.CreateEncryptor();
    }

    [ThreadStatic] static Random? _random;

    // This wraps the _random variable to make sure each thread gets a Random for itself
    static Random RandomInstance
    {
        get
        {
            if (_random == null)
                _random = new Random();

            return _random;
        }
    }

    public int CalcEncryptedSizeFor(ReadOnlySpan<byte> plainInput)
    {
        return NonceSize + TagSize + plainInput.Length;
    }

    public void Encrypt(ReadOnlySpan<byte> plainInput, Span<byte> outputBuffer)
    {
        lock (_lock)
        {
            RandomInstance.NextBytes(outputBuffer.Slice(0, NonceSize));
            outputBuffer[0] &= 0x0f; // 4 bits left for future algorithm type
            _aes.Encrypt(outputBuffer.Slice(0, NonceSize), plainInput,
                outputBuffer.Slice(NonceSize + TagSize, plainInput.Length), outputBuffer.Slice(NonceSize, TagSize));
        }
    }

    public int CalcPlainSizeFor(ReadOnlySpan<byte> encryptedInput)
    {
        return Math.Max(0, encryptedInput.Length - NonceSize - TagSize);
    }

    public bool Decrypt(ReadOnlySpan<byte> encryptedInput, Span<byte> outputBuffer)
    {
        if (encryptedInput.Length < NonceSize + TagSize)
            return false;
        if ((encryptedInput[0] & 0xf0) != 0) return false;
        try
        {
            lock (_lock)
            {
                _aes.Decrypt(encryptedInput.Slice(0, NonceSize),
                    encryptedInput.Slice(NonceSize + TagSize, outputBuffer.Length),
                    encryptedInput.Slice(NonceSize, TagSize),
                    outputBuffer);
            }
        }
        catch (CryptographicException)
        {
            return false;
        }

        return true;
    }

    public int CalcOrderedEncryptedSizeFor(ReadOnlySpan<byte> plainInput)
    {
        return (plainInput.Length + 15) & ~15;
    }

    public void OrderedEncrypt(ReadOnlySpan<byte> plainInput, Span<byte> outputBuffer)
    {
        var input = plainInput.ToArray();
        byte[] output;
        lock (_lock)
        {
            output = _encryptor.TransformFinalBlock(input, 0, input.Length);
        }

        output.CopyTo(outputBuffer);
    }

    public int CalcOrderedPlainSizeFor(ReadOnlySpan<byte> encryptedInput)
    {
        return encryptedInput.Length;
    }

    public bool OrderedDecrypt(ReadOnlySpan<byte> encryptedInput, Span<byte> outputBuffer)
    {
        var input = encryptedInput.ToArray();
        var output = new byte[input.Length];
        lock (_lock)
        {
            _decryptor.TransformBlock(input, 0, input.Length, output, 0);
        }

        output.CopyTo(outputBuffer);
        return true;
    }
}
