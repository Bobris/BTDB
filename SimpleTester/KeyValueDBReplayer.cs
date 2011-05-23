using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BTDB.KVDBLayer.Implementation;
using BTDB.KVDBLayer.Interface;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.KVDBLayer.ReplayProxy;
using BTDB.StreamLayer;

namespace SimpleTester
{
    class KeyValueDBReplayer
    {
        AbstractBufferedReader _reader;
        IKeyValueDB _db = new KeyValueDB();
        IPositionLessStream _dbstream;
        Dictionary<uint, IKeyValueDBTransaction> _trs = new Dictionary<uint, IKeyValueDBTransaction>();

        public KeyValueDBReplayer(string logname)
        {
            _reader = new PositionLessStreamReader(new PositionLessStreamProxy(logname));
        }

        public void Replay()
        {
            while (!_reader.Eof)
            {
                var operation = (KVReplayOperation)_reader.ReadUInt8();
                Console.WriteLine(operation);
                uint tri;
                IKeyValueDBTransaction tr;
                switch (operation)
                {
                    case KVReplayOperation.Open:
                        var initialStreamSize = _reader.ReadVUInt64();
                        _dbstream = new MemoryPositionLessStream();
                        var buf = new byte[4096];
                        ulong pos = 0;
                        while (pos < initialStreamSize)
                        {
                            var r = (int)Math.Min((ulong)buf.Length, initialStreamSize - pos);
                            _reader.ReadBlock(buf, 0, r);
                            _dbstream.Write(buf, 0, r, pos);
                            pos += (ulong)r;
                        }
                        _db.Open(_dbstream, true);
                        break;
                    case KVReplayOperation.KeyValueDBDispose:
                        _db.Dispose();
                        break;
                    case KVReplayOperation.StartTransaction:
                        tr = _db.StartTransaction();
                        tri = _reader.ReadVUInt32();
                        _trs[tri] = tr;
                        break;
                    case KVReplayOperation.CalculateStats:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        tr.CalculateStats();
                        break;
                    case KVReplayOperation.FindKey:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        int keyLen = _reader.ReadVInt32();
                        int keyOfs = _reader.ReadVInt32();
                        var keyBuf = new byte[keyOfs + keyLen];
                        _reader.ReadBlock(keyBuf, keyOfs, keyLen);
                        var strategy = (FindKeyStrategy)_reader.ReadVUInt32();
                        tr.FindKey(keyBuf, keyOfs, keyLen, strategy);
                        break;
                    case KVReplayOperation.GetKeyIndex:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        tr.GetKeyIndex();
                        break;
                    case KVReplayOperation.SetValue:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        int valOfs = _reader.ReadVInt32();
                        int valLen = _reader.ReadVInt32();
                        var valBuf = new byte[valOfs + valLen];
                        _reader.ReadBlock(valBuf, valOfs, valLen);
                        tr.SetValue(valBuf,valOfs,valLen);
                        break;
                    case KVReplayOperation.Commit:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        tr.Commit();
                        break;
                    case KVReplayOperation.TransactionDispose:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        tr.Dispose();
                        _trs.Remove(tri);
                        break;
                    case KVReplayOperation.EraseCurrent:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        tr.EraseCurrent();
                        break;
                    case KVReplayOperation.FindPreviousKey:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        tr.FindPreviousKey();
                        break;
                    case KVReplayOperation.FindNextKey:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        tr.FindNextKey();
                        break;

                    default:
                        Console.WriteLine(string.Format("Unimplemented operation {0}({1})", operation, (byte)operation));
                        throw new NotSupportedException(string.Format("Unimplemented operation {0}({1})", operation, (byte)operation));
                }
            }
            Console.WriteLine("Finish");
        }
    }
}
