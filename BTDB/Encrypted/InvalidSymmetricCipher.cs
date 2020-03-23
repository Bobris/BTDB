using System;

namespace BTDB.Encrypted
{
    public class InvalidSymmetricCipher: ISymmetricCipher
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
    }
}
