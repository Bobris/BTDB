using System;
using System.Security.Cryptography;

namespace BTDB.Encrypted;

public class AesGcmSymmetricCipher : ISymmetricCipher
{
    public const int KeySize = 32;
    const int NonceSize = 12;
    const int TagSize = 16;
    readonly AesGcm _aes;
    readonly Aes _aesCbc;
    static readonly byte[] ZeroIV = new byte[16];
    readonly object _lock = new object();

    public AesGcmSymmetricCipher(byte[] key)
    {
        _aes = new AesGcm(key, TagSize);
        _aesCbc = Aes.Create();
        _aesCbc.Key = key;
    }

    public int CalcEncryptedSizeFor(ReadOnlySpan<byte> plainInput)
    {
        return NonceSize + TagSize + plainInput.Length;
    }

    public void Encrypt(ReadOnlySpan<byte> plainInput, Span<byte> outputBuffer)
    {
        lock (_lock)
        {
            // A GCM nonce must never repeat for a key, so it comes from the cryptographic generator
            RandomNumberGenerator.Fill(outputBuffer.Slice(0, NonceSize));
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
        lock (_lock)
        {
            _aesCbc.EncryptCbc(plainInput, ZeroIV, outputBuffer, PaddingMode.Zeros);
        }
    }

    public int CalcOrderedPlainSizeFor(ReadOnlySpan<byte> encryptedInput)
    {
        return encryptedInput.Length;
    }

    public bool OrderedDecrypt(ReadOnlySpan<byte> encryptedInput, Span<byte> outputBuffer)
    {
        // Whole blocks only: damaged data fails the decryption instead of throwing.
        if ((encryptedInput.Length & 15) != 0) return false;
        // Every value is encrypted on its own with a zero IV, so each decryption must start a new CBC chain.
        lock (_lock)
        {
            _aesCbc.DecryptCbc(encryptedInput, ZeroIV, outputBuffer, PaddingMode.None);
        }

        return true;
    }
}
