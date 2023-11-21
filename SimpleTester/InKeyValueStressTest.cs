using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace SimpleTester;

public class ApiKey
{
    [PrimaryKey(1)] public ulong CompanyId { get; set; }
    [PrimaryKey(2)] public ulong ApiKeyId { get; set; }

    [SecondaryKey("Key", IncludePrimaryKeyOrder = 1)]
    public string Key { get; set; }

    public string Name { get; set; }
    public string Description { get; set; }
    public DateTime CreatedDate { get; set; }
    [InKeyValue(3)] public DateTime? LastUsedDate { get; set; }
    [InKeyValue(4)] public DateTime? ExpirationDate { get; set; }
    public ulong AdminRoleId { get; set; }
    public ISet<ulong> AdminRoleIds { get; set; } = new HashSet<ulong>();
    public ulong OwnerUserId { get; set; }
    public ApiKeyIpFiltering IpFiltering { get; set; }

    public bool IsUserSpecific()
    {
        return OwnerUserId != default;
    }

    public bool IsOwnerUser(ulong userId)
    {
        return OwnerUserId == userId;
    }

    public void RemoveRole(ulong roleId)
    {
        AdminRoleIds.Remove(roleId);
    }

    public bool HasAnyRoles() => AdminRoleIds.Count > 0;
}

public interface IApiKeyTable : IRelation<ApiKey>
{
    int RemoveById(ulong companyId);
    void Insert(ApiKey apiKey);
    void Update(ApiKey apiKey);
    bool UpdateById(ulong companyId, ulong apiKeyId, DateTime? lastUsedDate, DateTime? expirationDate);
    bool RemoveById(ulong companyId, ulong apiKeyId);
    bool Contains(ulong companyId, ulong apiKeyId);
    bool AnyByKey(ulong companyId, string key);
    ApiKey FindById(ulong companyId, ulong apiKeyId);
    ApiKey? FindByIdOrDefault(ulong companyId, ulong apiKeyId);
    ApiKey? FindByKeyOrDefault(ulong companyId, string key);
    ApiKeyInKeyValues? FindByIdOrDefaultOnlyInKeyValues(ulong companyId, ulong apiKeyId);
    IEnumerable<ApiKeyInKeyValues> ListById();
    IEnumerable<ApiKey> ListById(ulong companyId);
    uint CountById(ulong companyId);
}

public class ApiKeyInKeyValues
{
    public ulong CompanyId { get; set; }
    public ulong ApiKeyId { get; set; }
    public DateTime? LastUsedDate { get; set; }
    public DateTime? ExpirationDate { get; set; }
}

public class ApiKeyIpFiltering
{
    public bool IsIpFilterActive { get; set; }
    public IList<ApiKeyIpFilter> IpAddresses { get; set; }
    public IList<ApiKeyIpFilterRange> IpAddressRanges { get; set; }
}

public class ApiKeyIpFilter
{
    public string Name { get; set; }
    public string IpAddress { get; set; }
}

public class ApiKeyIpFilterRange
{
    public string Name { get; set; }
    public string IpAddressFrom { get; set; }
    public string IpAddressTo { get; set; }
}

public class InKeyValueStressTest
{
    readonly IKeyValueDB _lowDb;
    readonly ObjectDB _odb;

    public InKeyValueStressTest()
    {
        _lowDb = new BTreeKeyValueDB(new KeyValueDBOptions()
        {
            CompactorScheduler = null,
            Compression = new NoCompressionStrategy(),
            FileCollection = new InMemoryFileCollection()
        });
        _odb = new ObjectDB();
        _odb.Open(_lowDb, false, new DBOptions());
    }

    public void Run()
    {
        Task.Run(() =>
        {
            while (true)
            {
                using var tr = _lowDb.StartReadOnlyTransaction();
                if (tr.FindFirstKey(new()))
                {
                    var prevKey = tr.GetKeyToArray();
                    while (tr.FindNextKey(new()))
                    {
                        var key = tr.GetKeyToArray();
                        if (key.SequenceEqual(prevKey))
                        {
                            throw new InvalidOperationException();
                        }

                        prevKey = key;
                    }
                }
            }
        });
        var r = new Random(42);
        var cmdId = 0;
        while (true)
        {
            cmdId++;
            if (cmdId % 100000 == 0)
            {
                Console.WriteLine(cmdId);
            }
            switch (r.Next(1, 3))
            {
                case 1:
                {
                    var a = new ApiKey()
                    {
                        CompanyId = (ulong)r.Next(20, 300),
                        ApiKeyId = (ulong)r.Next(40, 500),
                        Name = "Test",
                        Key = "abcdefgh",
                        Description = "Test",
                        CreatedDate = new DateTime(2023, 10, 1, 2, 3, 4, DateTimeKind.Utc),
                    };
                    using var tr = _odb.StartTransaction();
                    var table = tr.GetRelation<IApiKeyTable>();
                    table.Upsert(a);
                    tr.Commit();
                    break;
                }
                case 2:
                {
                    using var tr = _odb.StartTransaction();
                    var table = tr.GetRelation<IApiKeyTable>();
                    var a = table.FindByIdOrDefaultOnlyInKeyValues((ulong)r.Next(20, 300), (ulong)r.Next(40, 500));
                    if (a != null)
                    {
                        var sec = r.Next(10, 50);
                        table.UpdateById(a.CompanyId, a.ApiKeyId,
                            new DateTime(2023, 10, 1, 2, 3, sec, DateTimeKind.Utc), null);
                    }

                    tr.Commit();
                    break;
                }
                case 3:
                {
                    if (r.Next(20) != 5) break;
                    using var tr = _odb.StartTransaction();
                    var table = tr.GetRelation<IApiKeyTable>();
                    table.RemoveById((ulong)r.Next(20, 300));
                    tr.Commit();
                    break;
                }
            }
        }
    }
}
