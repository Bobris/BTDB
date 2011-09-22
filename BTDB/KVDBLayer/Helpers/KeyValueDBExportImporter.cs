using System;
using System.IO;
using BTDB.Buffer;

namespace BTDB.KVDBLayer
{
    public static class KeyValueDBExportImporter
    {
        /// <summary>
        /// Writes all key value pairs in current prefix to stream (prefix itself is not written)
        /// </summary>
        /// <param name="transaction">transaction from where export all data</param>
        /// <param name="stream">where to write it to</param>
        public static void Export(IKeyValueDBTransaction transaction, Stream stream)
        {
            if (transaction == null) throw new ArgumentNullException("transaction");
            if (stream == null) throw new ArgumentNullException("stream");
            if (!stream.CanWrite) throw new ArgumentException("stream must be writeable", "stream");
            var keyValueCount = transaction.GetKeyValueCount();
            var tempbuf = new byte[16];
            tempbuf[0] = (byte)'B';
            tempbuf[1] = (byte)'T';
            tempbuf[2] = (byte)'D';
            tempbuf[3] = (byte)'B';
            tempbuf[4] = (byte)'E';
            tempbuf[5] = (byte)'X';
            tempbuf[6] = (byte)'P';
            tempbuf[7] = (byte)'1';
            PackUnpack.PackInt64LE(tempbuf, 8, keyValueCount);
            stream.Write(tempbuf, 0, 16);
            transaction.FindFirstKey();
            for (long kv = 0; kv < keyValueCount; kv++)
            {
                var keySize = transaction.GetKeySize();
                PackUnpack.PackInt32LE(tempbuf, 0, keySize);
                stream.Write(tempbuf, 0, 4);
                long ofs = 0;
                while (ofs < keySize)
                {
                    int len;
                    byte[] buf;
                    int bufOfs;
                    transaction.PeekKey((int)ofs, out len, out buf, out bufOfs);
                    stream.Write(buf, bufOfs, len);
                    ofs += len;
                }
                var valueSize = transaction.GetValueSize();
                PackUnpack.PackInt64LE(tempbuf, 0, valueSize);
                stream.Write(tempbuf, 0, 8);
                ofs = 0;
                while (ofs < valueSize)
                {
                    int len;
                    byte[] buf;
                    int bufOfs;
                    transaction.PeekValue(ofs, out len, out buf, out bufOfs);
                    stream.Write(buf, bufOfs, len);
                    ofs += len;
                }
                transaction.FindNextKey();
            }
        }

        /// <summary>
        /// Reads and inserts all key value pairs into current prefix from stream
        /// </summary>
        /// <param name="transaction">transaction where to import all data</param>
        /// <param name="stream">where to read it from</param>
        public static void Import(IKeyValueDBTransaction transaction, Stream stream)
        {
            if (transaction == null) throw new ArgumentNullException("transaction");
            if (stream == null) throw new ArgumentNullException("stream");
            if (!stream.CanRead) throw new ArgumentException("stream must be readable", "stream");
            var tempbuf = new byte[4096];
            if (stream.Read(tempbuf, 0, 16) != 16) throw new EndOfStreamException();
            if (tempbuf[0] != 'B' || tempbuf[1] != 'T' || tempbuf[2] != 'D' || tempbuf[3] != 'B' || tempbuf[4] != 'E' || tempbuf[5] != 'X' || tempbuf[6] != 'P' || tempbuf[7] != '1')
            {
                throw new BTDBException("Invalid header (it should start with BTDBEXP1)");
            }
            var keyValuePairs = PackUnpack.UnpackInt64LE(tempbuf, 8);
            if (keyValuePairs < 0) throw new BTDBException("Negative number of key value pairs");
            for (var kv = 0; kv < keyValuePairs; kv++)
            {
                if (stream.Read(tempbuf, 0, 4) != 4) throw new EndOfStreamException();
                var keySize = PackUnpack.UnpackInt32LE(tempbuf, 0);
                if (keySize < 0) throw new BTDBException("Negative key size");
                if (keySize > tempbuf.Length) tempbuf = new byte[keySize];
                if (stream.Read(tempbuf, 0, keySize) != keySize) throw new EndOfStreamException();
                transaction.CreateKey(tempbuf);
                if (stream.Read(tempbuf, 0, 8) != 8) throw new EndOfStreamException();
                var valueSize = PackUnpack.UnpackInt64LE(tempbuf, 0);
                if (valueSize < 0) throw new BTDBException("Negative value size");
                if (valueSize <= tempbuf.Length)
                {
                    if (stream.Read(tempbuf, 0, (int)valueSize) != valueSize) throw new EndOfStreamException();
                    transaction.SetValue(tempbuf, 0, (int)valueSize);
                }
                else
                {
                    transaction.SetValueSize(valueSize);
                    long pos = 0;
                    while (pos < valueSize)
                    {
                        int toRead = (int)Math.Min(valueSize - pos, tempbuf.Length);
                        if (stream.Read(tempbuf, 0, toRead) != toRead) throw new EndOfStreamException();
                        transaction.WriteValue(pos,toRead,tempbuf,0);
                        pos += toRead;
                    }
                }
            }
        }
    }
}
