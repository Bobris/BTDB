using System;
using System.Collections.Generic;
using System.Text;
using BTDB.KVDBLayer;
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
                int valOfs;
                int valLen;
                byte[] valBuf;
                int keyLen;
                int keyOfs;
                byte[] keyBuf;
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
                    case KVReplayOperation.StartWritingTransaction:
                        tr = _db.StartWritingTransaction().Result;
                        tri = _reader.ReadVUInt32();
                        _trs[tri] = tr;
                        break;
                    case KVReplayOperation.CalculateStats:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        tr.CalculateStats();
                        break;
                    case KVReplayOperation.SetKeyPrefix:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        int prefixLen = _reader.ReadVInt32();
                        int prefixOfs = _reader.ReadVInt32();
                        var prefixBuf = new byte[prefixOfs + prefixLen];
                        _reader.ReadBlock(prefixBuf, prefixOfs, prefixLen);
                        tr.SetKeyPrefix(prefixBuf, prefixOfs, prefixLen);
                        break;
                    case KVReplayOperation.FindKey:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        keyLen = _reader.ReadVInt32();
                        keyOfs = _reader.ReadVInt32();
                        keyBuf = new byte[keyOfs + keyLen];
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
                        valOfs = _reader.ReadVInt32();
                        valLen = _reader.ReadVInt32();
                        valBuf = new byte[valOfs + valLen];
                        _reader.ReadBlock(valBuf, valOfs, valLen);
                        tr.SetValue(valBuf, valOfs, valLen);
                        break;
                    case KVReplayOperation.SetValueSize:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        var valueSize = _reader.ReadVInt64();
                        tr.SetValueSize(valueSize);
                        break;
                    case KVReplayOperation.WriteValue:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        var ofs = _reader.ReadVInt64();
                        valOfs = _reader.ReadVInt32();
                        valLen = _reader.ReadVInt32();
                        valBuf = new byte[valOfs + valLen];
                        _reader.ReadBlock(valBuf, valOfs, valLen);
                        tr.WriteValue(ofs, valLen, valBuf, valOfs);
                        break;
                    case KVReplayOperation.CreateOrUpdateKeyValue:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        keyLen = _reader.ReadVInt32();
                        keyOfs = _reader.ReadVInt32();
                        keyBuf = new byte[keyOfs + keyLen];
                        _reader.ReadBlock(keyBuf, keyOfs, keyLen);
                        valLen = _reader.ReadVInt32();
                        valOfs = _reader.ReadVInt32();
                        valBuf = new byte[valOfs + valLen];
                        _reader.ReadBlock(valBuf, valOfs, valLen);
                        tr.CreateOrUpdateKeyValue(keyBuf, keyOfs, keyLen, valBuf, valOfs, valLen);
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
                    case KVReplayOperation.EraseAll:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        tr.EraseAll();
                        break;
                    case KVReplayOperation.FindFirstKey:
                        tri = _reader.ReadVUInt32();
                        tr = _trs[tri];
                        tr.FindFirstKey();
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

        public void CreateSource()
        {
            using (var os = new System.IO.StreamWriter("source.cs", false, Encoding.UTF8))
            {
                while (!_reader.Eof)
                {
                    var operation = (KVReplayOperation)_reader.ReadUInt8();
                    Console.WriteLine(operation);
                    uint tri;
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
                            os.WriteLine("var db=new KeyValueDB();");
                            os.WriteLine("var dbstream = new MemoryPositionLessStream();");
                            os.WriteLine("db.Open(dbstream,true);");
                            break;
                        case KVReplayOperation.KeyValueDBDispose:
                            os.WriteLine("db.Dispose();");
                            break;
                        case KVReplayOperation.StartTransaction:
                            tri = _reader.ReadVUInt32();
                            os.WriteLine(string.Format("var tr{0}=db.StartTransaction();", tri));
                            break;
                        case KVReplayOperation.CalculateStats:
                            tri = _reader.ReadVUInt32();
                            break;
                        case KVReplayOperation.FindKey:
                            tri = _reader.ReadVUInt32();
                            int keyLen = _reader.ReadVInt32();
                            int keyOfs = _reader.ReadVInt32();
                            var keyBuf = new byte[keyLen];
                            _reader.ReadBlock(keyBuf, 0, keyLen);
                            var strategy = (FindKeyStrategy)_reader.ReadVUInt32();
                            var content = new StringBuilder();
                            content.Append("{");
                            if (keyBuf.Length != 0)
                            {
                                foreach (var b in keyBuf)
                                {
                                    content.Append(b);
                                    content.Append(",");
                                }
                                content.Length--;
                            }
                            content.Append("}");
                            if (strategy == FindKeyStrategy.Create)
                            {
                                os.WriteLine(string.Format("tr{0}.CreateKey(new byte[] {1});", tri, content));
                            }
                            else
                            {
                                throw new NotImplementedException();
                            }
                            break;
                        case KVReplayOperation.GetKeyIndex:
                            tri = _reader.ReadVUInt32();
                            break;
                        case KVReplayOperation.SetValue:
                            tri = _reader.ReadVUInt32();
                            int valOfs = _reader.ReadVInt32();
                            int valLen = _reader.ReadVInt32();
                            _reader.SkipBlock(valLen);
                            os.WriteLine(string.Format("tr{0}.SetValue(new byte[{1}]);", tri, valLen));
                            break;
                        case KVReplayOperation.Commit:
                            tri = _reader.ReadVUInt32();
                            os.WriteLine(string.Format("tr{0}.Commit();", tri));
                            break;
                        case KVReplayOperation.TransactionDispose:
                            tri = _reader.ReadVUInt32();
                            os.WriteLine(string.Format("tr{0}.Dispose();", tri));
                            break;
                        case KVReplayOperation.EraseCurrent:
                            tri = _reader.ReadVUInt32();
                            os.WriteLine(string.Format("tr{0}.EraseCurrent();", tri));
                            break;
                        case KVReplayOperation.FindPreviousKey:
                            tri = _reader.ReadVUInt32();
                            os.WriteLine(string.Format("tr{0}.FindPreviousKey();", tri));
                            break;
                        case KVReplayOperation.FindNextKey:
                            tri = _reader.ReadVUInt32();
                            os.WriteLine(string.Format("tr{0}.FindNextKey();", tri));
                            break;

                        default:
                            Console.WriteLine(string.Format("Unimplemented operation {0}({1})", operation, (byte)operation));
                            throw new NotSupportedException(string.Format("Unimplemented operation {0}({1})", operation, (byte)operation));
                    }
                }
            }
        }
    }
}
