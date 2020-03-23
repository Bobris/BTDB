using System;

namespace BTDB.Encrypted
{
    public interface ISymmetricCipher
    {
        int CalcEncryptedSizeFor(ReadOnlySpan<byte> plainInput);
        void Encrypt(ReadOnlySpan<byte> plainInput, Span<byte> outputBuffer);

        int CalcPlainSizeFor(ReadOnlySpan<byte> encryptedInput);
        // returns true if checksum matches
        bool Decrypt(ReadOnlySpan<byte> encryptedInput, Span<byte> outputBuffer);
    }
}
