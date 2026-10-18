using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Allocators;
using BTDB.KVDBLayer;
using BTDB.KVDBLayer.ReadOnly;
using Xunit;

namespace BTDBTest;

// Model based test: the same random operation sequence is executed against every key value DB implementation
// and against simple sorted list model. All observable results must match the model.
public class KeyValueDBDifferentialTest
{
    public enum Kind
    {
        InMemory,
        BTreeInMemoryFiles,
        BTreeOnDisk,
        BTreeMemoryMapped
    }

    sealed class DbHolder : IDisposable
    {
        readonly Kind _kind;
        readonly string? _dir;
        IFileCollection? _files;
        readonly LeakDetectorWrapperAllocator _allocator = new(new MallocAllocator());
        public IKeyValueDB Db = null!;

        public DbHolder(Kind kind)
        {
            _kind = kind;
            if (kind is Kind.BTreeOnDisk or Kind.BTreeMemoryMapped)
            {
                _dir = Path.Combine(Path.GetTempPath(), "BTDBDiff" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(_dir);
            }

            Open();
        }

        public bool Persistent => _kind != Kind.InMemory;

        void Open()
        {
            switch (_kind)
            {
                case Kind.InMemory:
                    Db = new InMemoryKeyValueDB();
                    return;
                case Kind.BTreeInMemoryFiles:
                    _files ??= new InMemoryFileCollection();
                    break;
                case Kind.BTreeOnDisk:
                    _files = new OnDiskFileCollection(_dir!);
                    break;
                case Kind.BTreeMemoryMapped:
                    _files = new OnDiskMemoryMappedFileCollection(_dir!);
                    break;
            }

            Db = new BTreeKeyValueDB(new KeyValueDBOptions
            {
                FileCollection = _files, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
                Allocator = _allocator, FileSplitSize = 64 * 1024
            });
        }

        public void Reopen()
        {
            Db.Dispose();
            if (_kind is Kind.BTreeOnDisk or Kind.BTreeMemoryMapped)
            {
                _files!.Dispose();
            }

            Open();
        }

        public void Dispose()
        {
            Db.Dispose();
            _files?.Dispose();
            if (_dir != null) Directory.Delete(_dir, true);
            Assert.Equal(0u, _allocator.QueryAllocations().Count);
        }
    }

    sealed class Model
    {
        public readonly List<byte[]> Keys = [];
        public readonly List<byte[]> Values = [];
        public ulong CommitUlong;
        public ulong[] Ulongs = new ulong[4];

        public Model Clone()
        {
            var res = new Model { CommitUlong = CommitUlong, Ulongs = (ulong[])Ulongs.Clone() };
            res.Keys.AddRange(Keys);
            res.Values.AddRange(Values);
            return res;
        }

        public int Count => Keys.Count;

        public int LowerBound(ReadOnlySpan<byte> key)
        {
            var l = 0;
            var r = Keys.Count;
            while (l < r)
            {
                var m = (l + r) / 2;
                if (Keys[m].AsSpan().SequenceCompareTo(key) < 0) l = m + 1;
                else r = m;
            }

            return l;
        }

        public int IndexOf(ReadOnlySpan<byte> key)
        {
            var i = LowerBound(key);
            return i < Keys.Count && Keys[i].AsSpan().SequenceEqual(key) ? i : -1;
        }

        public bool Upsert(byte[] key, byte[] value)
        {
            var i = LowerBound(key);
            if (i < Keys.Count && Keys[i].AsSpan().SequenceEqual(key))
            {
                Values[i] = value;
                return false;
            }

            Keys.Insert(i, key);
            Values.Insert(i, value);
            return true;
        }

        public void RemoveRange(int from, int to)
        {
            Keys.RemoveRange(from, to - from + 1);
            Values.RemoveRange(from, to - from + 1);
        }

        public int FirstWithPrefix(ReadOnlySpan<byte> prefix)
        {
            var i = LowerBound(prefix);
            return i < Keys.Count && Keys[i].AsSpan().StartsWith(prefix) ? i : -1;
        }

        public int LastWithPrefix(ReadOnlySpan<byte> prefix)
        {
            var i = FirstWithPrefix(prefix);
            if (i < 0) return -1;
            while (i + 1 < Keys.Count && Keys[i + 1].AsSpan().StartsWith(prefix)) i++;
            return i;
        }

        // index of first key strictly bigger than key
        public int UpperBound(ReadOnlySpan<byte> key)
        {
            var i = LowerBound(key);
            if (i < Keys.Count && Keys[i].AsSpan().SequenceEqual(key)) i++;
            return i;
        }
    }

    sealed class CursorModel
    {
        public byte[]? Key;
        public bool Removed;

        public void Set(byte[]? key)
        {
            Key = key;
            Removed = false;
        }
    }

    sealed class Runner
    {
        readonly Random _rnd;
        readonly DbHolder _holder;
        readonly int _seed;
        readonly bool _allowHugeKeys;
        readonly bool _bigKeys;
        Model _committed = new();
        Model _current = null!;
        IKeyValueDBTransaction? _tr;
        readonly IKeyValueDBCursor?[] _cursors = new IKeyValueDBCursor?[3];
        readonly CursorModel[] _cm = [new(), new(), new()];
        IKeyValueDBTransaction? _snapshotTr;
        Model? _snapshotModel;
        int _step;
        readonly List<string> _log = [];

        void Log(string what)
        {
            _log.Add($"{_step}: {what}");
            if (_log.Count > 30) _log.RemoveAt(0);
        }

        static string Hex(ReadOnlySpan<byte> data) =>
            data.Length > 16 ? Convert.ToHexString(data[..16]) + "..(" + data.Length + ")" : Convert.ToHexString(data);

        string CursorStates() => string.Join(", ",
            _cm.Select((m, i) => $"c{i}=" + (m.Key == null ? "invalid" : (m.Removed ? "removed " : "") + Hex(m.Key))));
        readonly byte[] _commonPrefix;

        public Runner(Kind kind, int seed, bool allowHugeKeys, bool bigKeys = false)
        {
            _bigKeys = bigKeys;
            _seed = seed;
            _rnd = new(seed);
            _holder = new(kind);
            _allowHugeKeys = allowHugeKeys;
            _commonPrefix = new byte[40];
            _rnd.NextBytes(_commonPrefix);
        }

        void Check(bool condition, string what)
        {
            Assert.True(condition,
                $"Seed {_seed} step {_step}: {what}\n{CursorStates()}\n{string.Join("\n", _log)}");
        }

        void CheckBytes(ReadOnlySpan<byte> expected, ReadOnlySpan<byte> actual, string what)
        {
            if (!expected.SequenceEqual(actual))
                Check(false,
                    $"{what} expected {Convert.ToHexString(expected[..Math.Min(32, expected.Length)])}(len {expected.Length}) actual {Convert.ToHexString(actual[..Math.Min(32, actual.Length)])}(len {actual.Length})");
        }

        void CheckEqual<T>(T expected, T actual, string what)
        {
            if (!EqualityComparer<T>.Default.Equals(expected, actual))
                Check(false, $"{what} expected {expected} actual {actual}");
        }

        byte[] NewKey()
        {
            var r = _rnd.Next(1000);
            if (r < 2 && _allowHugeKeys)
            {
                var huge = new byte[66000 + _rnd.Next(5000)];
                _rnd.NextBytes(huge);
                huge[0] = (byte)_rnd.Next(3);
                return huge;
            }

            if (_bigKeys && r < 700)
            {
                // Many big keys with long common prefix fill in memory leaves over their compressed limit
                var big = new byte[1500 + _rnd.Next(2500)];
                _rnd.NextBytes(big);
                _commonPrefix.CopyTo(big, 0);
                big[_commonPrefix.Length] = (byte)_rnd.Next(4);
                return big;
            }

            if (r < 60)
            {
                var big = new byte[300 + _rnd.Next(4000)];
                _rnd.NextBytes(big);
                big[0] = (byte)_rnd.Next(3);
                return big;
            }

            byte[] alphabet = [0, 1, 2, 128, 255];
            var len = _rnd.Next(13);
            var withCommon = _rnd.Next(3) == 0;
            var key = new byte[(withCommon ? _commonPrefix.Length : 0) + len];
            if (withCommon) _commonPrefix.CopyTo(key, 0);
            for (var i = withCommon ? _commonPrefix.Length : 0; i < key.Length; i++)
                key[i] = alphabet[_rnd.Next(alphabet.Length)];
            return key;
        }

        byte[] NewValue()
        {
            var r = _rnd.Next(100);
            var len = r switch
            {
                < 45 => _rnd.Next(8),
                < 75 => 8 + _rnd.Next(50),
                < 95 => 50 + _rnd.Next(500),
                _ => 1000 + _rnd.Next(20000)
            };
            var value = new byte[len];
            _rnd.NextBytes(value);
            return value;
        }

        byte[] ExistingKeyOrNew()
        {
            if (_current.Count > 0 && _rnd.Next(3) == 0) return _current.Keys[_rnd.Next(_current.Count)];
            return NewKey();
        }

        byte[] RandomPrefix()
        {
            switch (_rnd.Next(4))
            {
                case 0:
                    return [];
                case 1 when _current.Count > 0:
                {
                    var k = _current.Keys[_rnd.Next(_current.Count)];
                    return k[..Math.Min(k.Length, _rnd.Next(Math.Min(k.Length, 45) + 1))];
                }
                default:
                {
                    var k = NewKey();
                    return k[..Math.Min(k.Length, _rnd.Next(4))];
                }
            }
        }

        void StartTransaction()
        {
            _current = _committed.Clone();
            _tr = _rnd.Next(3) switch
            {
                0 => _holder.Db.StartWritingTransaction().AsTask().GetAwaiter().GetResult(),
                _ => _holder.Db.StartTransaction()
            };
            for (var i = 0; i < _cursors.Length; i++)
            {
                _cursors[i] = _tr.CreateCursor();
                _cm[i].Set(null);
            }
        }

        void FinishTransaction(bool commit)
        {
            foreach (var c in _cursors) c!.Dispose();
            if (commit)
            {
                _tr!.Commit();
                _committed = _current;
            }

            _tr!.Dispose();
            _tr = null;
        }

        public async Task Run(int steps)
        {
            try
            {
                StartTransaction();
                for (_step = 0; _step < steps; _step++)
                {
                    try
                    {
                        await Step();
                    }
                    catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                    {
                        Check(false, ex.ToString());
                    }
                }

                FinishTransaction(true);
                VerifyCommitted();
                if (_holder.Persistent)
                {
                    DisposeSnapshot();
                    _holder.Reopen();
                    VerifyCommitted();
                    await _holder.Db.Compact(CancellationToken.None);
                    VerifyCommitted();
                }

                VerifyReadOnlyCopy();
            }
            finally
            {
                DisposeSnapshot();
                if (_tr != null) FinishTransaction(false);
                _holder.Dispose();
            }
        }

        void DisposeSnapshot()
        {
            _snapshotTr?.Dispose();
            _snapshotTr = null;
            _snapshotModel = null;
        }

        async Task Step()
        {
            var ci = _rnd.Next(_cursors.Length);
            var c = _cursors[ci]!;
            var cm = _cm[ci];
            var op = _rnd.Next(100);
            Log($"op {op} on c{ci} states {CursorStates()}");
            switch (op)
            {
                case < 24:
                    DoCreateOrUpdate(c, ci);
                    break;
                case < 27:
                    if (IsAt(cm)) DoSetValue(c, cm);
                    break;
                case < 33:
                    if (IsAt(cm)) DoEraseCurrent(c, ci);
                    break;
                case < 35:
                {
                    var ti = _rnd.Next(_cursors.Length);
                    if (ti != ci && IsAt(cm) && IsAt(_cm[ti])) DoEraseUpTo(ci, ti);
                    break;
                }
                case < 40:
                    DoFindFirst(c, cm);
                    break;
                case < 44:
                    DoFindLast(c, cm);
                    break;
                case < 52:
                    DoFindNext(c, cm);
                    break;
                case < 58:
                    DoFindPrevious(c, cm);
                    break;
                case < 65:
                    DoFind(c, cm);
                    break;
                case < 68:
                    DoFindKeyIndex(c, cm);
                    break;
                case < 71:
                    DoFindKeyIndexWithPrefix(c, cm);
                    break;
                case < 74:
                    DoUpdateKeySuffix(c, ci);
                    break;
                case < 75:
                    c.Invalidate();
                    cm.Set(null);
                    break;
                case < 77:
                    DoFastIterate(c, cm);
                    break;
                case < 80:
                    if (IsAt(cm)) DoKeyHasPrefix(c, cm);
                    break;
                case < 82:
                    DoUlongs();
                    break;
                case < 86:
                    FinishTransaction(true);
                    MaybeSnapshot();
                    StartTransaction();
                    break;
                case < 88:
                    FinishTransaction(false);
                    StartTransaction();
                    break;
                case < 89:
                    FinishTransaction(true);
                    if (_holder.Persistent)
                    {
                        DisposeSnapshot();
                        _holder.Reopen();
                    }

                    StartTransaction();
                    break;
                case < 90:
                    FinishTransaction(true);
                    VerifySnapshot();
                    await _holder.Db.Compact(CancellationToken.None);
                    StartTransaction();
                    break;
                case < 92:
                    VerifyCurrentByIteration(c, cm);
                    break;
                default:
                    foreach (var (cur, m) in _cursors.Zip(_cm)) CheckCursor(cur!, m);
                    break;
            }

            CheckCursor(_cursors[ci]!, cm);
        }

        static bool IsAt(CursorModel cm) => cm.Key != null && !cm.Removed;

        void CheckCursor(IKeyValueDBCursor c, CursorModel cm)
        {
            if (cm.Key == null)
            {
                Check(!c.IsValid(), "cursor should be invalid");
                Check(c.GetKeyIndex() == -1, "invalid cursor should have key index -1");
                return;
            }

            if (cm.Removed) return;
            Check(c.IsValid(), "cursor should be valid");
            var idx = _current.IndexOf(cm.Key);
            Check(idx >= 0, "model key exists");
            CheckEqual((long)idx, c.GetKeyIndex(), "key index");
            Span<byte> buf = stackalloc byte[16];
            CheckBytes(cm.Key, c.GetKeySpan(ref buf), "key");
            CheckBytes(_current.Values[idx], c.GetValueSpan(ref buf), "value");
            switch (_rnd.Next(4))
            {
                case 0:
                {
                    Memory<byte> mb = new byte[8];
                    CheckBytes(cm.Key, c.GetKeyMemory(ref mb, true).Span, "key memory copy");
                    CheckBytes(_current.Values[idx], c.GetValueMemory(ref mb, true).Span, "value memory copy");
                    break;
                }
                case 1:
                {
                    Memory<byte> mb = default;
                    CheckBytes(cm.Key, c.GetKeyMemory(ref mb).Span, "key memory");
                    CheckBytes(_current.Values[idx], c.GetValueMemory(ref mb).Span, "value memory");
                    break;
                }
                case 2:
                {
                    var big = new byte[cm.Key.Length + _rnd.Next(2)];
                    CheckBytes(cm.Key, c.GetKeySpan(big.AsSpan(), true), "key span copy");
                    Span<byte> small = stackalloc byte[1];
                    CheckBytes(cm.Key, c.GetKeySpan(small, true), "key span copy small");
                    Span<byte> sb = stackalloc byte[2];
                    CheckBytes(_current.Values[idx], c.GetValueSpan(ref sb, true), "value span copy");
                    CheckBytes(cm.Key, c.GetKeySpan(ref sb, true), "key span ref copy");
                    break;
                }
                default:
                {
                    var size = c.GetStorageSizeOfCurrentKey();
                    CheckEqual((uint)cm.Key.Length, size.Key, "storage key size");
                    Check(!c.IsValueCorrupted(), "value is not corrupted");
                    break;
                }
            }
        }

        void DoCreateOrUpdate(IKeyValueDBCursor c, int ci)
        {
            var key = ExistingKeyOrNew();
            var value = NewValue();
            var created = c.CreateOrUpdateKeyValue(key, value);
            Log($"CreateOrUpdate {Hex(key)} value len {value.Length} -> {created}");
            var expectedCreated = _current.Upsert(key, value);
            CheckEqual(expectedCreated, created, "created");
            _cm[ci].Set(key);
            if (!created) return;
            for (var i = 0; i < _cm.Length; i++)
            {
                if (i == ci) continue;
                if (_cm[i].Removed && _cm[i].Key.AsSpan().SequenceEqual(key)) _cm[i].Removed = false;
            }
        }

        void DoSetValue(IKeyValueDBCursor c, CursorModel cm)
        {
            var value = NewValue();
            c.SetValue(value);
            _current.Values[_current.IndexOf(cm.Key)] = value;
        }

        void DoEraseCurrent(IKeyValueDBCursor c, int ci)
        {
            var key = _cm[ci].Key!;
            var idx = _current.IndexOf(key);
            c.EraseCurrent();
            Log($"EraseCurrent {Hex(key)}");
            _current.RemoveRange(idx, idx);
            foreach (var m in _cm)
            {
                if (m.Key != null && !m.Removed && m.Key.AsSpan().SequenceEqual(key)) m.Removed = true;
            }
        }

        void DoEraseUpTo(int ci, int ti)
        {
            var from = _current.IndexOf(_cm[ci].Key);
            var to = _current.IndexOf(_cm[ti].Key);
            var removed = _cursors[ci]!.EraseUpTo(_cursors[ti]!);
            Log($"EraseUpTo c{ci}..c{ti} -> {removed}");
            if (to < from)
            {
                CheckEqual(0L, removed, "erase up to backwards");
                return;
            }

            CheckEqual((long)(to - from + 1), removed, "erase up to count");
            var removedKeys = _current.Keys.GetRange(from, to - from + 1);
            _current.RemoveRange(from, to);
            foreach (var m in _cm)
            {
                if (m.Key == null || m.Removed) continue;
                var k = m.Key;
                if (removedKeys.Any(rk => rk.AsSpan().SequenceEqual(k))) m.Removed = true;
            }
        }

        void ResultPosition(CursorModel cm, bool found, int idx)
        {
            if (found) cm.Set(_current.Keys[idx]);
            else cm.Set(null);
        }

        void DoFindFirst(IKeyValueDBCursor c, CursorModel cm)
        {
            var prefix = RandomPrefix();
            var expected = _current.FirstWithPrefix(prefix);
            var res = c.FindFirstKey(prefix);
            Log($"FindFirstKey {Hex(prefix)} -> {res}");
            CheckEqual(expected >= 0, res, "FindFirstKey");
            ResultPosition(cm, res, expected);
        }

        void DoFindLast(IKeyValueDBCursor c, CursorModel cm)
        {
            var prefix = RandomPrefix();
            var expected = _current.LastWithPrefix(prefix);
            var res = c.FindLastKey(prefix);
            Log($"FindLastKey {Hex(prefix)} -> {res}");
            CheckEqual(expected >= 0, res, "FindLastKey");
            ResultPosition(cm, res, expected);
        }

        byte[] PrefixForMove(CursorModel cm)
        {
            if (cm.Key == null || _rnd.Next(2) == 0) return RandomPrefix();
            return cm.Key[..Math.Min(cm.Key.Length, _rnd.Next(Math.Min(cm.Key.Length, 45) + 1))];
        }

        void DoFindNext(IKeyValueDBCursor c, CursorModel cm)
        {
            var prefix = PrefixForMove(cm);
            int expected;
            if (cm.Key == null)
            {
                expected = _current.FirstWithPrefix(prefix);
            }
            else
            {
                expected = _current.UpperBound(cm.Key);
                if (expected >= _current.Count || !_current.Keys[expected].AsSpan().StartsWith(prefix)) expected = -1;
            }

            var res = c.FindNextKey(prefix);
            Log($"FindNextKey {Hex(prefix)} -> {res}");
            CheckEqual(expected >= 0, res, "FindNextKey");
            ResultPosition(cm, res, expected);
        }

        void DoFindPrevious(IKeyValueDBCursor c, CursorModel cm)
        {
            var prefix = PrefixForMove(cm);
            int expected;
            if (cm.Key == null)
            {
                expected = _current.LastWithPrefix(prefix);
            }
            else
            {
                expected = _current.LowerBound(cm.Key) - 1;
                if (expected < 0 || !_current.Keys[expected].AsSpan().StartsWith(prefix)) expected = -1;
            }

            var res = c.FindPreviousKey(prefix);
            Log($"FindPreviousKey {Hex(prefix)} -> {res}");
            CheckEqual(expected >= 0, res, "FindPreviousKey");
            ResultPosition(cm, res, expected);
        }

        void DoFind(IKeyValueDBCursor c, CursorModel cm)
        {
            var key = _rnd.Next(4) == 0 && _current.Count > 0
                ? _current.Keys[_rnd.Next(_current.Count)]
                : NewKey();
            if (_rnd.Next(3) == 0 && key.Length > 0)
            {
                // Slightly modify existing key to land between keys
                key = key.ToArray();
                key[^1] ^= 1;
            }

            var prefixLen = (uint)_rnd.Next(Math.Min(key.Length, 45) + 1);
            var prefix = key.AsSpan(0, (int)prefixLen);
            var exact = _current.IndexOf(key);
            FindResult expected;
            int expectedIdx;
            if (exact >= 0)
            {
                expected = FindResult.Exact;
                expectedIdx = exact;
            }
            else
            {
                var ins = _current.LowerBound(key);
                if (ins > 0 && _current.Keys[ins - 1].AsSpan().StartsWith(prefix))
                {
                    expected = FindResult.Previous;
                    expectedIdx = ins - 1;
                }
                else if (ins < _current.Count && _current.Keys[ins].AsSpan().StartsWith(prefix))
                {
                    expected = FindResult.Next;
                    expectedIdx = ins;
                }
                else
                {
                    expected = FindResult.NotFound;
                    expectedIdx = -1;
                }
            }

            var res = c.Find(key, prefixLen);
            Log($"Find {Hex(key)} {prefixLen} -> {res}");
            CheckEqual(expected, res, "Find");
            ResultPosition(cm, res != FindResult.NotFound, expectedIdx);
        }

        void DoFindKeyIndex(IKeyValueDBCursor c, CursorModel cm)
        {
            var index = (long)_rnd.Next(-2, _current.Count + 3);
            var res = c.FindKeyIndex(index);
            Log($"FindKeyIndex {index} -> {res}");
            var expected = index >= 0 && index < _current.Count;
            CheckEqual(expected, res, "FindKeyIndex");
            ResultPosition(cm, res, (int)index);
        }

        void DoFindKeyIndexWithPrefix(IKeyValueDBCursor c, CursorModel cm)
        {
            var prefix = RandomPrefix();
            var first = _current.FirstWithPrefix(prefix);
            var last = _current.LastWithPrefix(prefix);
            var index = (long)_rnd.Next(-2, (first >= 0 ? last - first + 1 : 0) + 3);
            var res = c.FindKeyIndex(prefix, index);
            Log($"FindKeyIndex {Hex(prefix)} {index} -> {res}");
            var expected = first >= 0 && index >= 0 && first + index <= last;
            CheckEqual(expected, res, "FindKeyIndex with prefix");
            ResultPosition(cm, res, first + (int)index);
        }

        void DoUpdateKeySuffix(IKeyValueDBCursor c, int ci)
        {
            // Removed cursors could change relative position with updated key, so make them invalid first
            for (var i = 0; i < _cm.Length; i++)
            {
                if (!_cm[i].Removed) continue;
                _cursors[i]!.Invalidate();
                _cm[i].Set(null);
            }

            byte[] key;
            uint prefixLen;
            if (_current.Count > 0 && _rnd.Next(5) != 0)
            {
                var old = _current.Keys[_rnd.Next(_current.Count)];
                prefixLen = (uint)_rnd.Next(old.Length + 1);
                var r = _rnd.Next(4);
                var nk = NewKey();
                var suffix = r == 0 ? [] : r == 1 ? old[(int)prefixLen..] : nk[..Math.Min(nk.Length, _rnd.Next(_bigKeys ? 3000 : 20))];
                key = [.. old.AsSpan(0, (int)prefixLen), .. suffix];
            }
            else
            {
                key = NewKey();
                prefixLen = (uint)_rnd.Next(key.Length + 1);
            }

            var prefix = key.AsSpan(0, (int)prefixLen);
            var first = _current.FirstWithPrefix(prefix);
            var last = _current.LastWithPrefix(prefix);
            UpdateKeySuffixResult expected;
            if (first < 0) expected = UpdateKeySuffixResult.NotFound;
            else if (first != last) expected = UpdateKeySuffixResult.NotUniquePrefix;
            else if (_current.Keys[first].AsSpan().SequenceEqual(key)) expected = UpdateKeySuffixResult.NothingToDo;
            else expected = UpdateKeySuffixResult.Updated;
            var res = c.UpdateKeySuffix(key, prefixLen);
            Log($"UpdateKeySuffix {Hex(key)} {prefixLen} -> {res}");
            CheckEqual(expected, res, "UpdateKeySuffix");
            switch (res)
            {
                case UpdateKeySuffixResult.NotFound:
                case UpdateKeySuffixResult.NotUniquePrefix:
                    _cm[ci].Set(null);
                    break;
                case UpdateKeySuffixResult.NothingToDo:
                    _cm[ci].Set(_current.Keys[first]);
                    break;
                case UpdateKeySuffixResult.Updated:
                {
                    var oldKey = _current.Keys[first];
                    _current.Keys[first] = key;
                    foreach (var m in _cm)
                    {
                        if (m.Key != null && m.Key.AsSpan().SequenceEqual(oldKey)) m.Key = key;
                    }

                    _cm[ci].Set(key);
                    break;
                }
            }
        }

        void DoFastIterate(IKeyValueDBCursor c, CursorModel cm)
        {
            var start = cm.Key == null ? 0 : cm.Removed ? _current.UpperBound(cm.Key) : _current.IndexOf(cm.Key);
            var limit = _rnd.Next(1, 80);
            var seen = new List<(long, byte[])>();
            Span<byte> buffer = stackalloc byte[_rnd.Next(2) == 0 ? 0 : 64];
            var noCursor = _rnd.Next(2) == 0;
            CursorIterateCallback callback = (idx, key) =>
            {
                seen.Add((idx, key.ToArray()));
                return seen.Count >= limit;
            };
            if (noCursor) c.FastIterateNoCursor(ref buffer, callback);
            else c.FastIterate(ref buffer, callback);
            var expectedCount = Math.Min(limit, _current.Count - start);
            CheckEqual(expectedCount, seen.Count, "FastIterate count");
            for (var i = 0; i < seen.Count; i++)
            {
                CheckEqual((long)(start + i), seen[i].Item1, "FastIterate index");
                CheckBytes(_current.Keys[start + i], seen[i].Item2, "FastIterate key");
            }

            if (seen.Count == limit)
            {
                cm.Set(_current.Keys[start + limit - 1]);
            }
            else
            {
                c.Invalidate();
                cm.Set(null);
            }
        }

        void DoKeyHasPrefix(IKeyValueDBCursor c, CursorModel cm)
        {
            var prefix = PrefixForMove(cm);
            CheckEqual(cm.Key.AsSpan().StartsWith(prefix), c.KeyHasPrefix(prefix), "KeyHasPrefix");
        }

        void DoUlongs()
        {
            CheckEqual(_current.CommitUlong, _tr!.GetCommitUlong(), "commit ulong");
            var idx = (uint)_rnd.Next(_current.Ulongs.Length);
            CheckEqual(_current.Ulongs[idx], _tr.GetUlong(idx), "ulong");
            if (_rnd.Next(2) == 0)
            {
                var v = (ulong)_rnd.Next(1000);
                _tr.SetCommitUlong(v);
                _current.CommitUlong = v;
            }
            else
            {
                var v = (ulong)_rnd.Next(1000);
                _tr.SetUlong(idx, v);
                _current.Ulongs[idx] = v;
            }
        }

        void MaybeSnapshot()
        {
            if (_snapshotTr != null)
            {
                VerifySnapshot();
                return;
            }

            _snapshotTr = _holder.Db.StartReadOnlyTransaction();
            _snapshotModel = _committed.Clone();
        }

        void VerifySnapshot()
        {
            if (_snapshotTr == null) return;
            VerifyTransaction(_snapshotTr, _snapshotModel!);
            DisposeSnapshot();
        }

        void VerifyCommitted()
        {
            using var tr = _holder.Db.StartReadOnlyTransaction();
            VerifyTransaction(tr, _committed);
        }

        void VerifyTransaction(IKeyValueDBTransaction tr, Model model)
        {
            CheckEqual((long)model.Count, tr.GetKeyValueCount(), "key value count");
            CheckEqual(model.CommitUlong, tr.GetCommitUlong(), "commit ulong");
            for (var i = 0u; i < model.Ulongs.Length; i++)
                CheckEqual(model.Ulongs[i], tr.GetUlong(i), "ulong " + i);
            using var c = tr.CreateCursor();
            var i2 = 0;
            Span<byte> buf = stackalloc byte[32];
            while (c.FindNextKey([]))
            {
                Check(i2 < model.Count, "too many keys");
                CheckBytes(model.Keys[i2], c.GetKeySpan(ref buf), "iterated key");
                CheckBytes(model.Values[i2], c.GetValueSpan(ref buf), "iterated value");
                i2++;
            }

            CheckEqual(model.Count, i2, "iterated count");
            i2 = model.Count - 1;
            while (c.FindPreviousKey([]))
            {
                CheckBytes(model.Keys[i2], c.GetKeySpan(ref buf), "backward iterated key");
                i2--;
            }

            CheckEqual(-1, i2, "backward iterated count");
            var stats = tr.CalcBTreeStats();
            if (model.Count > 0) Check(stats.Count > 0, "has some btree stats");
        }

        void VerifyCurrentByIteration(IKeyValueDBCursor c, CursorModel cm)
        {
            CheckEqual((long)_current.Count, _tr!.GetKeyValueCount(), "current count");
            var i = 0;
            Span<byte> buf = stackalloc byte[32];
            if (c.FindFirstKey([]))
            {
                do
                {
                    CheckBytes(_current.Keys[i], c.GetKeySpan(ref buf), "current iterated key");
                    i++;
                } while (c.FindNextKey([]));
            }

            CheckEqual(_current.Count, i, "current iterated count");
            cm.Set(null);
        }

        void VerifyReadOnlyCopy()
        {
            using var tr = _holder.Db.StartReadOnlyTransaction();
            var creator = new ReadOnlyCreator(tr);
            creator.SetMaxChildren((uint)_rnd.Next(2, 40));
            if (_committed.Keys.Any(k => k.Length > ReadOnlyFormat.MaxNodeKeyBytes))
            {
                Assert.Throws<BTDBException>(() => creator.WriteToArray());
                return;
            }

            using var ro = ReadOnlyKeyValueDB.Create(creator.WriteToArray());
            using var roTr = ro.StartReadOnlyTransaction();
            VerifyTransaction(roTr, _committed);
            // Exercise read only cursor operations against model
            _current = _committed;
            using var c = roTr.CreateCursor();
            var cm = new CursorModel();
            for (var i = 0; i < 300; i++)
            {
                _step = 100000 + i;
                switch (_rnd.Next(8))
                {
                    case 0: DoFindFirst(c, cm); break;
                    case 1: DoFindLast(c, cm); break;
                    case 2: DoFindNext(c, cm); break;
                    case 3: DoFindPrevious(c, cm); break;
                    case 4: DoFind(c, cm); break;
                    case 5: DoFindKeyIndex(c, cm); break;
                    case 6: DoFindKeyIndexWithPrefix(c, cm); break;
                    default:
                        if (cm.Key != null) DoKeyHasPrefix(c, cm);
                        else DoFastIterate(c, cm);
                        break;
                }

                CheckCursor(c, cm);
            }
        }
    }

    public static IEnumerable<object[]> Cases()
    {
        foreach (var kind in Enum.GetValues<Kind>())
        {
            for (var seed = 1; seed <= 5; seed++)
            {
                yield return [kind, seed];
            }
        }
    }

    [Theory]
    [InlineData(Kind.InMemory, 11)]
    [InlineData(Kind.InMemory, 12)]
    [InlineData(Kind.BTreeInMemoryFiles, 11)]
    public async Task RandomOperationsWithBigKeysMatchModel(Kind kind, int seed)
    {
        var runner = new Runner(kind, seed, allowHugeKeys: false, bigKeys: true);
        await runner.Run(2000);
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task RandomOperationsMatchModel(Kind kind, int seed)
    {
        var runner = new Runner(kind, seed, allowHugeKeys: seed == 3);
        await runner.Run(3000);
    }
}
