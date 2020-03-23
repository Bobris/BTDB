using System;
using System.Security.Cryptography;

namespace BTDB.Encrypted
{
    public class AesGcmSymmetricCipher : ISymmetricCipher
    {
        public const int KeySize = 32;
        const int NonceSize = 12;
        const int TagSize = 16;
        readonly AesGcm _aes;

        public AesGcmSymmetricCipher(byte[] key)
        {
            _aes = new AesGcm(key);
        }

        public AesGcmSymmetricCipher(ReadOnlySpan<byte> key)
        {
            _aes = new AesGcm(key);
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
            RandomInstance.NextBytes(outputBuffer.Slice(0, NonceSize));
            outputBuffer[0] &= 0x0f; // 4 bits left for future algorithm type
            _aes.Encrypt(outputBuffer.Slice(0, NonceSize), plainInput,
                outputBuffer.Slice(NonceSize + TagSize, plainInput.Length), outputBuffer.Slice(NonceSize, TagSize));
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
                _aes.Decrypt(encryptedInput.Slice(0, NonceSize),
                    encryptedInput.Slice(NonceSize + TagSize, outputBuffer.Length),
                    encryptedInput.Slice(NonceSize, TagSize),
                    outputBuffer);
            }
            catch (CryptographicException)
            {
                return false;
            }

            return true;
        }
    }
}
