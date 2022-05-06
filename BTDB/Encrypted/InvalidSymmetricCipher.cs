using System;

namespace BTDB.Encrypted;

public class InvalidSymmetricCipher : ISymmetricCipher
{
    public int CalcEncryptedSizeFor(ReadOnlySpan<byte> plainInput)
    {
        throw new NotSupportedException();
    }

    public void Encrypt(ReadOnlySpan<byte> plainInput, Span<byte> outputBuffer)
    {
        throw new NotSupportedException();
    }

    public int CalcPlainSizeFor(ReadOnlySpan<byte> encryptedInput)
    {
        throw new NotSupportedException();
    }

    public bool Decrypt(ReadOnlySpan<byte> encryptedInput, Span<byte> outputBuffer)
    {
        throw new NotSupportedException();
    }

    public int CalcOrderedEncryptedSizeFor(ReadOnlySpan<byte> plainInput)
    {
        throw new NotSupportedException();
    }

    public void OrderedEncrypt(ReadOnlySpan<byte> plainInput, Span<byte> outputBuffer)
    {
        throw new NotSupportedException();
    }

    public int CalcOrderedPlainSizeFor(ReadOnlySpan<byte> encryptedInput)
    {
        throw new NotSupportedException();
    }

    public bool OrderedDecrypt(ReadOnlySpan<byte> encryptedInput, Span<byte> outputBuffer)
    {
        throw new NotSupportedException();
    }
}
