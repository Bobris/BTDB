using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    internal class ChunkStorageInKV : IChunkStorage
    {
        struct StorageValue
        {
            internal uint FileId;
            internal uint FileOfs;
            internal uint ContentLengthCompressedIsLeaf;
            internal uint ContentLength
            {
                get { return ContentLengthCompressedIsLeaf / 4; }
                set { ContentLengthCompressedIsLeaf = (ContentLengthCompressedIsLeaf & 1) + value * 4; }
            }

            internal bool Compressed
            {
                get { return (ContentLengthCompressedIsLeaf & 2) != 0; }
                set
                {
                    if (value) ContentLengthCompressedIsLeaf |= 2u; else ContentLengthCompressedIsLeaf &= ~2u;
                }
            }

            internal bool Leaf
            {
                get { return (ContentLengthCompressedIsLeaf & 1) != 0; }
                set
                {
                    if (value) ContentLengthCompressedIsLeaf |= 1u; else ContentLengthCompressedIsLeaf &= ~1u;
                }
            }
        }

        readonly long _subDBId;
        readonly IFileCollectionWithFileInfos _fileCollection;
        readonly long _maxFileSize;
        volatile int _keyLen;
        readonly ConcurrentDictionary<ByteStructs.Key20, StorageValue> _dict20 = new ConcurrentDictionary<ByteStructs.Key20, StorageValue>(new ByteStructs.Key20EqualityComparer());
        readonly object _pureValueFileLock = new object();
        IFileCollectionFile _pureValueFile;
        AbstractBufferedWriter _pureValueFileWriter;
        IFileCollectionFile _hashIndexFile;
        AbstractBufferedWriter _hashIndexWriter;

        public ChunkStorageInKV(long subDBId, IFileCollectionWithFileInfos fileCollection, long maxFileSize)
        {
            _subDBId = subDBId;
            _fileCollection = fileCollection;
            _maxFileSize = maxFileSize;
            _keyLen = -1;
            LoadFiles();
        }

        void LoadFiles()
        {
            var hashKeyIndexFiles = new List<KeyValuePair<uint, long>>();
            var fileIdOfLastPureValues = 0u;
            var generationOfLastPureValues = 0L;
            foreach (var pair in _fileCollection.FileInfos)
            {
                if (pair.Value.SubDBId != _subDBId) continue;
                if (pair.Value.FileType == KV2FileType.PureValuesWithId)
                {
                    if (generationOfLastPureValues < pair.Value.Generation)
                    {
                        generationOfLastPureValues = pair.Value.Generation;
                        fileIdOfLastPureValues = pair.Key;
                    }
                }
                else if (pair.Value.FileType == KV2FileType.HashKeyIndex)
                {
                    hashKeyIndexFiles.Add(new KeyValuePair<uint, long>(pair.Key, pair.Value.Generation));
                }
            }
            if (hashKeyIndexFiles.Count == 0)
                return;
            hashKeyIndexFiles.Sort((x, y) => x.Value < y.Value ? -1 : x.Value > y.Value ? 1 : 0);
            TryLoadHashKeyIndex(hashKeyIndexFiles[hashKeyIndexFiles.Count - 1].Key);
        }

        bool TryLoadHashKeyIndex(uint hashKeyIndexFileId)
        {
            var reader = _fileCollection.GetFile(hashKeyIndexFileId).GetExclusiveReader();
            _keyLen = (int) ((IHashKeyIndex)_fileCollection.FileInfoByIdx(hashKeyIndexFileId)).KeyLen;
            HashKeyIndex.SkipHeader(reader);
            var keyBuf = ByteBuffer.NewSync(new byte[_keyLen]);
            while (!reader.Eof)
            {
                var value = new StorageValue();
                value.FileId = reader.ReadVUInt32();
                value.FileOfs = reader.ReadVUInt32();
                value.ContentLengthCompressedIsLeaf = reader.ReadVUInt32();
                reader.ReadBlock(keyBuf);
                _dict20.TryAdd(new ByteStructs.Key20(keyBuf), value);
            }
            return true;
        }

        void CheckOrInitKeyLen(int keyLen)
        {
            if (_keyLen == -1)
            {
                if (keyLen != 20) throw new ArgumentException("Length of Key must be 20 bytes");
                Interlocked.CompareExchange(ref _keyLen, keyLen, -1);
            }
            if (_keyLen != keyLen)
            {
                throw new ArgumentException("Key length is different from stored");
            }
        }

        public IChunkStorageTransaction StartTransaction()
        {
            return new ChunkStorageTransaction(this);
        }

        class ChunkStorageTransaction : IChunkStorageTransaction
        {
            readonly ChunkStorageInKV _chunkStorageInKV;

            public ChunkStorageTransaction(ChunkStorageInKV chunkStorageInKV)
            {
                _chunkStorageInKV = chunkStorageInKV;
            }

            public void Dispose()
            {
                lock (_chunkStorageInKV._pureValueFileLock)
                {
                    _chunkStorageInKV.FlushFiles();
                }
            }

            public void Put(ByteBuffer key, ByteBuffer content, bool isLeaf)
            {
                _chunkStorageInKV.CheckOrInitKeyLen(key.Length);
                var key20 = new ByteStructs.Key20(key);
                var d = _chunkStorageInKV._dict20;
                StorageValue val;
            again:
                if (d.TryGetValue(key20, out val))
                {
                    if (val.ContentLength != content.Length) throw new InvalidOperationException("Hash collision or error in memory");
                    if (!isLeaf && val.Leaf)
                    {
                        var newval = val;
                        newval.Leaf = false;
                        if (!d.TryUpdate(key20, newval, val)) goto again;
                        lock (_chunkStorageInKV._pureValueFileLock)
                        {
                            _chunkStorageInKV.StoreHashUpdate(key, newval);
                        }
                    }
                    return;
                }
                lock (_chunkStorageInKV._pureValueFileLock)
                {
                    val = _chunkStorageInKV.StoreContent(content);
                    val.Leaf = isLeaf;
                    if (!d.TryAdd(key20, val))
                    {
                        goto again;
                    }
                    _chunkStorageInKV.StoreHashUpdate(key, val);
                }
            }

            public Task<ByteBuffer> Get(ByteBuffer key)
            {
                _chunkStorageInKV.CheckOrInitKeyLen(key.Length);
                var tcs = new TaskCompletionSource<ByteBuffer>();
                var key20 = new ByteStructs.Key20(key);
                var d = _chunkStorageInKV._dict20;
                StorageValue val;
                if (d.TryGetValue(key20, out val))
                {
                    var buf = new byte[val.ContentLength];
                    _chunkStorageInKV._fileCollection.GetFile(val.FileId).RandomRead(buf, 0, buf.Length, val.FileOfs);
                    tcs.SetResult(ByteBuffer.NewAsync(buf));
                }
                else
                {
                    tcs.SetResult(ByteBuffer.NewEmpty());
                }
                return tcs.Task;
            }
        }

        void FlushFiles()
        {
            if (_pureValueFileWriter != null)
            {
                _pureValueFileWriter.FlushBuffer();
                _pureValueFile.HardFlush();                
            }
            if (_hashIndexWriter != null)
            {
                _hashIndexWriter.FlushBuffer();
                _hashIndexFile.HardFlush();                
            }
        }

        StorageValue StoreContent(ByteBuffer content)
        {
            var result = new StorageValue();
            result.Compressed = false;
            result.ContentLength = (uint)content.Length;
            if (_pureValueFile == null)
                StartNewPureValueFile();
            result.FileId = _pureValueFile.Index;
            result.FileOfs = (uint)_pureValueFileWriter.GetCurrentPosition();
            _pureValueFileWriter.WriteBlock(content);
            if (_pureValueFileWriter.GetCurrentPosition() >= _maxFileSize)
            {
                _pureValueFileWriter.FlushBuffer();
                StartNewPureValueFile();
            }
            return result;
        }

        void StartNewPureValueFile()
        {
            _pureValueFile = _fileCollection.AddFile("hpv");
            _pureValueFileWriter = _pureValueFile.GetAppenderWriter();
            var fileInfo = new FilePureValuesWithId(_subDBId, _fileCollection.NextGeneration());
            fileInfo.WriteHeader(_pureValueFileWriter);
            _fileCollection.SetInfo(_pureValueFile.Index, fileInfo);
        }

        void StoreHashUpdate(ByteBuffer key, StorageValue storageValue)
        {
            if (_hashIndexWriter == null)
            {
                StartNewHashIndexFile();
            }
            _hashIndexWriter.WriteVUInt32(storageValue.FileId);
            _hashIndexWriter.WriteVUInt32(storageValue.FileOfs);
            _hashIndexWriter.WriteVUInt32(storageValue.ContentLengthCompressedIsLeaf);
            _hashIndexWriter.WriteBlock(key);
        }

        void StartNewHashIndexFile()
        {
            _hashIndexFile = _fileCollection.AddFile("hid");
            _hashIndexWriter = _hashIndexFile.GetAppenderWriter();
            var fileInfo = new HashKeyIndex(_subDBId, _fileCollection.NextGeneration(), (uint)_keyLen);
            fileInfo.WriteHeader(_hashIndexWriter);
            _fileCollection.SetInfo(_hashIndexFile.Index, fileInfo);
        }
    }
}
