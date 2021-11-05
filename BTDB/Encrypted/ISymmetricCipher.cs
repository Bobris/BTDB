using System;

namespace BTDB.Encrypted;

public interface ISymmetricCipher
{
    int CalcEncryptedSizeFor(ReadOnlySpan<byte> plainInput);
    void Encrypt(ReadOnlySpan<byte> plainInput, Span<byte> outputBuffer);

    int CalcPlainSizeFor(ReadOnlySpan<byte> encryptedInput);
    // returns true if checksum matches
    bool Decrypt(ReadOnlySpan<byte> encryptedInput, Span<byte> outputBuffer);

    int CalcOrderedEncryptedSizeFor(ReadOnlySpan<byte> plainInput);
    void OrderedEncrypt(ReadOnlySpan<byte> plainInput, Span<byte> outputBuffer);

    int CalcOrderedPlainSizeFor(ReadOnlySpan<byte> encryptedInput);
    // returns true if checksum matches
    bool OrderedDecrypt(ReadOnlySpan<byte> encryptedInput, Span<byte> outputBuffer);
}
