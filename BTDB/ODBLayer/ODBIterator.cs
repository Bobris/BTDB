using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public class ODBIterator
    {
        readonly IInternalObjectDBTransaction _tr;
        readonly IODBVisitor _visitor;
        Dictionary<uint, string> _tableId2Name;
        readonly IKeyValueDBTransaction _trkv;
        Dictionary<uint, ulong> _singletons;
        readonly HashSet<uint> _usedTableIds;
        readonly byte[] _tempBytes = new byte[32];
        readonly HashSet<ulong> _visitedOids;
        readonly HashSet<TableIdVersion> _usedTableVersions;
        readonly Dictionary<TableIdVersion, TableVersionInfo> _tableVersionInfos;

        struct TableIdVersion : IEquatable<TableIdVersion>
        {
            readonly uint _tableid;
            readonly uint _version;

            public TableIdVersion(uint tableid, uint version)
            {
                _tableid = tableid;
                _version = version;
            }

            public bool Equals(TableIdVersion other)
            {
                return _tableid == other._tableid && _version == other._version;
            }

            public override int GetHashCode()
            {
                return (int)(_tableid * 33 + _version);
            }
        }
        public ODBIterator(IObjectDBTransaction tr, IODBVisitor visitor)
        {
            _tr = (IInternalObjectDBTransaction)tr;
            _trkv = _tr.KeyValueDBTransaction;
            _visitor = visitor;
            _usedTableIds = new HashSet<uint>();
            _visitedOids = new HashSet<ulong>();
            _usedTableVersions = new HashSet<TableIdVersion>();
            _tableVersionInfos = new Dictionary<TableIdVersion, TableVersionInfo>();
        }

        public void Iterate()
        {
            LoadTableNamesDict();
            MarkLastDictId();
            _trkv.SetKeyPrefixUnsafe(ObjectDB.TableSingletonsPrefix);
            var keyReader = new KeyValueDBKeyReader(_trkv);
            var valueReader = new KeyValueDBValueReader(_trkv);
            _singletons = new Dictionary<uint, ulong>();
            while (_trkv.FindNextKey())
            {
                keyReader.Restart();
                valueReader.Restart();
                _singletons.Add(keyReader.ReadVUInt32(), valueReader.ReadVUInt64());
            }
            foreach (var singleton in _singletons)
            {
                string name;
                if (
                    !_visitor.VisitSingleton(singleton.Key,
                        _tableId2Name.TryGetValue(singleton.Key, out name) ? name : null, singleton.Value))
                    continue;
                MarkTableName(singleton.Key);
                _trkv.SetKeyPrefixUnsafe(ObjectDB.TableSingletonsPrefix);
                if (_trkv.Find(Vuint2ByteBuffer(singleton.Key)) == FindResult.Exact)
                {
                    _visitor.MarkCurrentKeyAsUsed(_trkv);
                }
                IterateOid(singleton.Value);
            }
        }

        void IterateOid(ulong oid)
        {
            if (!_visitedOids.Add(oid))
                return;
            _trkv.SetKeyPrefix(ObjectDB.AllObjectsPrefix);
            if (_trkv.Find(Vuint2ByteBuffer(oid)) != FindResult.Exact)
                return; // Object oid was deleted
            _visitor.MarkCurrentKeyAsUsed(_trkv);
            var reader = new KeyValueDBValueReader(_trkv);
            var tableId = reader.ReadVUInt32();
            var version = reader.ReadVUInt32();
            MarkTableIdVersionFieldInfo(tableId, version);
            string tableName;
            if (!_visitor.StartObject(oid, tableId, _tableId2Name.TryGetValue(tableId, out tableName) ? tableName : null,
                version))
                return;
            var tvi = GetTableVersionInfo(tableId, version);
            // TODO
            _visitor.EndObject();
        }

        void MarkTableIdVersionFieldInfo(uint tableId, uint version)
        {
            if (!_usedTableVersions.Add(new TableIdVersion(tableId, version)))
                return;
            _trkv.SetKeyPrefixUnsafe(ObjectDB.TableVersionsPrefix);
            if (_trkv.Find(TwiceVuint2ByteBuffer(tableId,version))==FindResult.Exact)
            {
                _visitor.MarkCurrentKeyAsUsed(_trkv);
            }
        }

        void MarkLastDictId()
        {
            _trkv.SetKeyPrefixUnsafe(null);
            if (_trkv.FindExactKey(ObjectDB.LastDictIdKey))
            {
                _visitor.MarkCurrentKeyAsUsed(_trkv);
            }
        }

        void MarkTableName(uint tableId)
        {
            if (!_usedTableIds.Add(tableId))
                return;
            _trkv.SetKeyPrefixUnsafe(ObjectDB.TableNamesPrefix);
            if (_trkv.Find(Vuint2ByteBuffer(tableId)) == FindResult.Exact)
            {
                _visitor.MarkCurrentKeyAsUsed(_trkv);
            }
        }

        ByteBuffer Vuint2ByteBuffer(uint v)
        {
            var ofs = 0;
            PackUnpack.PackVUInt(_tempBytes, ref ofs, v);
            return ByteBuffer.NewSync(_tempBytes, 0, ofs);
        }

        ByteBuffer Vuint2ByteBuffer(ulong v)
        {
            var ofs = 0;
            PackUnpack.PackVUInt(_tempBytes, ref ofs, v);
            return ByteBuffer.NewSync(_tempBytes, 0, ofs);
        }

        ByteBuffer TwiceVuint2ByteBuffer(uint v1,uint v2)
        {
            var ofs = 0;
            PackUnpack.PackVUInt(_tempBytes, ref ofs, v1);
            PackUnpack.PackVUInt(_tempBytes, ref ofs, v2);
            return ByteBuffer.NewSync(_tempBytes, 0, ofs);
        }

        void LoadTableNamesDict()
        {
            _tableId2Name = ObjectDB.LoadTablesEnum(_tr.KeyValueDBTransaction).ToDictionary(pair => pair.Key, pair => pair.Value);
        }

        TableVersionInfo GetTableVersionInfo(uint tableId, uint version)
        {
            TableVersionInfo res;
            if (_tableVersionInfos.TryGetValue(new TableIdVersion(tableId, version), out res))
                return res;
            _trkv.SetKeyPrefixUnsafe(ObjectDB.TableVersionsPrefix);
            if (_trkv.Find(TwiceVuint2ByteBuffer(tableId, version)) == FindResult.Exact)
            {
                var reader = new KeyValueDBValueReader(_trkv);
                res = TableVersionInfo.Load(reader, _tr.Owner.FieldHandlerFactory, _tableId2Name[tableId]);
                return res;
            }
            throw new ArgumentException($"TableVersionInfo not found {tableId}-{version}");
        }
    }
}
