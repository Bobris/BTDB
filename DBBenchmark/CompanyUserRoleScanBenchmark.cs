using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace DBBenchmark;

[Generate]
public class CompanyUserRole
{
    [PrimaryKey(1)] public ulong CompanyId { get; set; }
    [PrimaryKey(2)] public ulong UserId { get; set; }
    [PrimaryKey(3)] public ulong RoleId { get; set; }
}

[Generate]
public class CompanyUserRoleIteration
{
    public ulong UserId { get; set; }
    public ulong RoleId { get; set; }
    [NotStored] public HashSet<ulong> RoleIds = null!;
    [NotStored] public HashSet<ulong> ActiveUserIds = null!;
    [NotStored] public Dictionary<ulong, List<ulong>>? UserIdsByRoleId;
    [NotStored] public int Count;
}

public interface ICompanyUserRoleTable : IRelation<CompanyUserRole>
{
    void Insert(CompanyUserRole item);
    IEnumerable<CompanyUserRole> FindById(ulong companyId);
    void IterateById(ulong companyId, Action<CompanyUserRoleIteration> callback, CompanyUserRoleIteration value);
}

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[ShortRunJob]
public class CompanyUserRoleScanBenchmark
{
    const ulong CompanyId = 1;
    const int PopulatedRoleCount = 10;
    const int EmptyRoleCount = 500;
    const int UsersPerRole = 35_000;

    InMemoryFileCollection _fileCollection = null!;
    ObjectDB _db = null!;
    IObjectDBTransaction _readOnlyTransaction = null!;
    ICompanyUserRoleTable _table = null!;
    HashSet<ulong> _roleIds = null!;
    HashSet<ulong> _activeUserIds = null!;
    CompanyUserRoleIteration _iterationValue = null!;
    long _keyChecksum;
    CursorIterateCallback _checksumCallback = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _checksumCallback = AccumulateKeyChecksum;
        _fileCollection = new InMemoryFileCollection();
        _db = new ObjectDB();
        _db.Open(new BTreeKeyValueDB(_fileCollection), true);

        _roleIds = new HashSet<ulong>(PopulatedRoleCount + EmptyRoleCount);
        for (var roleId = 1; roleId <= PopulatedRoleCount + EmptyRoleCount; roleId++)
            _roleIds.Add((ulong)roleId);

        _activeUserIds = new HashSet<ulong>(UsersPerRole);
        using (var transaction = _db.StartTransaction())
        {
            var table = transaction.GetRelation<ICompanyUserRoleTable>();
            for (var userId = 1; userId <= UsersPerRole; userId++)
            {
                _activeUserIds.Add((ulong)userId);
                for (var roleId = 1; roleId <= PopulatedRoleCount; roleId++)
                {
                    table.Insert(new()
                    {
                        CompanyId = CompanyId,
                        UserId = (ulong)userId,
                        RoleId = (ulong)roleId
                    });
                }
            }

            transaction.Commit();
        }

        _readOnlyTransaction = _db.StartReadOnlyTransaction();
        _table = _readOnlyTransaction.GetRelation<ICompanyUserRoleTable>();
        _iterationValue = new()
        {
            RoleIds = _roleIds,
            ActiveUserIds = _activeUserIds
        };
    }

    [Benchmark]
    public long IterateBTreeKeys()
    {
        using var cursor = _readOnlyTransaction.KeyValueDBTransaction.CreateCursor();
        Span<byte> buffer = stackalloc byte[2048];
        long totalKeyBytes = 0;
        cursor.FastIterate(ref buffer, (_, key) =>
        {
            totalKeyBytes += key.Length;
            return false;
        });
        return totalKeyBytes;
    }

    [Benchmark]
    public long IterateBTreeKeysNoCursor()
    {
        using var cursor = _readOnlyTransaction.KeyValueDBTransaction.CreateCursor();
        Span<byte> buffer = stackalloc byte[2048];
        long totalKeyBytes = 0;
        cursor.FastIterateNoCursor(ref buffer, (_, key) =>
        {
            totalKeyBytes += key.Length;
            return false;
        });
        return totalKeyBytes;
    }

    [Benchmark]
    public long ScanBTreeKeysNoCursor()
    {
        using var cursor = _readOnlyTransaction.KeyValueDBTransaction.CreateCursor();
        Span<byte> buffer = stackalloc byte[2048];
        cursor.FastIterateNoCursor(ref buffer, static (_, _) => false);
        return cursor.GetKeyIndex();
    }

    [Benchmark]
    public long IterateBTreeKeysNoCursorChecksum()
    {
        using var cursor = _readOnlyTransaction.KeyValueDBTransaction.CreateCursor();
        Span<byte> buffer = stackalloc byte[2048];
        _keyChecksum = 0;
        cursor.FastIterateNoCursor(ref buffer, _checksumCallback);
        return _keyChecksum;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    bool AccumulateKeyChecksum(long keyIndex, ReadOnlySpan<byte> key)
    {
        _keyChecksum += keyIndex + key.Length;
        if (!key.IsEmpty) _keyChecksum += key[0] + key[^1];
        return false;
    }

    [Benchmark(Baseline = true)]
    public int Iterate()
    {
        var count = 0;
        foreach (var companyUserRole in _table.FindById(CompanyId))
        {
            if (!_roleIds.Contains(companyUserRole.RoleId) || !_activeUserIds.Contains(companyUserRole.UserId))
                continue;

            count++;
        }

        return count;
    }

    [Benchmark]
    public int IterateWithReusedValue()
    {
        _iterationValue.Count = 0;
        _table.IterateById(CompanyId, CountActiveRole, _iterationValue);
        return _iterationValue.Count;
    }

    static void CountActiveRole(CompanyUserRoleIteration value)
    {
        if (!value.RoleIds.Contains(value.RoleId) || !value.ActiveUserIds.Contains(value.UserId))
            return;

        value.Count++;
    }

    [Benchmark]
    public Dictionary<ulong, List<ulong>> IterateAndCreateDictionary()
    {
        var userIdsByRoleId = new Dictionary<ulong, List<ulong>>();
        foreach (var companyUserRole in _table.FindById(CompanyId))
        {
            if (!_roleIds.Contains(companyUserRole.RoleId) || !_activeUserIds.Contains(companyUserRole.UserId))
                continue;

            if (!userIdsByRoleId.TryGetValue(companyUserRole.RoleId, out var userIds))
            {
                userIds = [];
                userIdsByRoleId.Add(companyUserRole.RoleId, userIds);
            }

            userIds.Add(companyUserRole.UserId);
        }

        return userIdsByRoleId;
    }

    [Benchmark]
    public Dictionary<ulong, List<ulong>> IterateAndCreateDictionaryWithReusedValue()
    {
        var userIdsByRoleId = new Dictionary<ulong, List<ulong>>();
        _iterationValue.UserIdsByRoleId = userIdsByRoleId;
        _table.IterateById(CompanyId, AddActiveRoleToDictionary, _iterationValue);
        return userIdsByRoleId;
    }

    static void AddActiveRoleToDictionary(CompanyUserRoleIteration value)
    {
        if (!value.RoleIds.Contains(value.RoleId) || !value.ActiveUserIds.Contains(value.UserId))
            return;

        var userIdsByRoleId = value.UserIdsByRoleId!;
        if (!userIdsByRoleId.TryGetValue(value.RoleId, out var userIds))
        {
            userIds = [];
            userIdsByRoleId.Add(value.RoleId, userIds);
        }

        userIds.Add(value.UserId);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _readOnlyTransaction.Dispose();
        _db.Dispose();
        _fileCollection.Dispose();
    }
}
