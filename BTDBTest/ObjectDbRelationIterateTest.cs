using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using BTDB;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ObjectDbRelationIterateTest
{
    [Generate]
    public class IterateRow
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong UserId { get; set; }
        [PrimaryKey(3)] public ulong RoleId { get; set; }
        public string Payload { get; set; } = "";
        public ReadOnlyMemory<byte> Data { get; set; }
        public List<string> Tags { get; set; } = [];
    }

    [Generate]
    public class IterateProjection
    {
        public ulong UserId { get; set; }
        public ulong RoleId { get; set; }
        [NotStored] public object? Context;
        [NotStored] public readonly List<IterateProjection> SeenInstances = [];
        [NotStored] public readonly List<(ulong UserId, ulong RoleId)> SeenValues = [];
    }

    [Generate]
    public class IterateProjectionWithPrefix
    {
        public ulong CompanyId { get; set; }
        public ulong UserId { get; set; }
        public ulong RoleId { get; set; }
        [NotStored] public object? Context;
    }

    [Generate]
    public class IterateValueProjection
    {
        public ulong CompanyId { get; set; }
        public ulong UserId { get; set; }
        public string Payload { get; set; } = "";
        public ReadOnlyMemory<byte> Data { get; set; }
        public List<string> Tags { get; set; } = [];
        [NotStored] public object? Context;
    }

    [Generate]
    public class IterateValueOnlyProjection
    {
        public string Payload { get; set; } = "";
        [NotStored] public object? Context;
    }

    [Generate]
    public interface IGeneratedIterateTable : IRelation<IterateRow>
    {
        void IterateById(ulong companyId, Action<IterateProjection> callback, IterateProjection value);
        void IterateById(ulong companyId, Action<IterateValueProjection> callback, IterateValueProjection value);
        void IterateById(ulong companyId, Action<IterateValueOnlyProjection> callback, IterateValueOnlyProjection value);
        void IterateById(ulong companyId, Action<IterateProjectionWithPrefix> callback,
            IterateProjectionWithPrefix value);
    }

    [Fact]
    public void SourceGeneratedIterateByIdReusesValueAndPreservesNotStoredContext()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = new ObjectDB();
        db.Open(new BTreeKeyValueDB(fileCollection), true);
        using var tr = db.StartTransaction();
        var table = tr.GetRelation<IGeneratedIterateTable>();
        Seed(table);

        AssertIteration((callback, value) => table.IterateById(1, callback, value));

        var context = new object();
        var value = new IterateProjectionWithPrefix { Context = context };
        var seenValues = new List<(ulong CompanyId, ulong UserId, ulong RoleId)>();
        table.IterateById(1, item =>
        {
            Assert.Same(value, item);
            Assert.Same(context, item.Context);
            seenValues.Add((item.CompanyId, item.UserId, item.RoleId));
        }, value);
        Assert.Equal([(1ul, 10ul, 1ul), (1ul, 10ul, 2ul), (1ul, 20ul, 1ul)], seenValues);
    }

    [Fact]
    public void ReflectionGeneratedIterateByIdReusesValueAndPreservesNotStoredContext()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = new ObjectDB();
        db.Open(new BTreeKeyValueDB(fileCollection), true);
        using var tr = db.StartTransaction();
        var interfaceType = CreateReflectionRelationInterface();
        var table = tr.GetRelation(interfaceType);
        Seed((IRelation<IterateRow>)table);
        var method = interfaceType.GetMethod("IterateById")!;

        AssertIteration((callback, value) => method.Invoke(table, [1ul, callback, value]));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IterateByIdDeserializesValuesIntoReusableProjection(bool reflectionGenerated)
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = new ObjectDB();
        db.Open(new BTreeKeyValueDB(fileCollection), true);
        var interfaceType = reflectionGenerated
            ? CreateReflectionRelationInterface(typeof(IterateValueProjection))
            : typeof(IGeneratedIterateTable);
        using (var tr = db.StartTransaction())
        {
            var table = (IRelation<IterateRow>)tr.GetRelation(interfaceType);
            // Cross leaf boundaries and exercise both inline and out-of-line stored values.
            for (ulong companyId = 0; companyId < 3; companyId++)
            for (ulong userId = 0; userId < 200; userId++)
            {
                table.Upsert(new()
                {
                    CompanyId = companyId, UserId = userId, RoleId = 1,
                    Payload = userId % 2 == 0 ? $"{companyId}/{userId}" : new string('x', 5000),
                    Data = new byte[] { (byte)companyId, (byte)userId },
                    Tags = userId % 2 == 0 ? ["tag", userId.ToString()] : []
                });
            }
            tr.Commit();
        }

        using var readTr = db.StartReadOnlyTransaction();
        var readTable = readTr.GetRelation(interfaceType);
        var method = interfaceType.GetMethod("IterateById",
            [typeof(ulong), typeof(Action<IterateValueProjection>), typeof(IterateValueProjection)])!;
        var context = new object();
        var value = new IterateValueProjection { Context = context };
        var seen = 0ul;
        var savedData = new List<ReadOnlyMemory<byte>>();
        Action<IterateValueProjection> callback = item =>
        {
            Assert.Same(value, item);
            Assert.Same(context, item.Context);
            Assert.Equal(1ul, item.CompanyId);
            Assert.Equal(seen, item.UserId);
            Assert.Equal(seen % 2 == 0 ? $"1/{seen}" : new string('x', 5000), item.Payload);
            Assert.Equal(new byte[] { 1, (byte)seen }, item.Data.ToArray());
            Assert.Equal(seen % 2 == 0 ? new[] { "tag", seen.ToString() } : [], item.Tags);
            savedData.Add(item.Data);
            seen++;
        };
        method.Invoke(readTable, [1ul, callback, value]);
        Assert.Equal(200ul, seen);
        for (var i = 0; i < savedData.Count; i++)
            Assert.Equal(new byte[] { 1, (byte)i }, savedData[i].ToArray());
        method.Invoke(readTable, [3ul, callback, value]);
        Assert.Equal(200ul, seen);
    }

    [Fact]
    public void IterateByIdSupportsProjectionWithOnlyValueFields()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = new ObjectDB();
        db.Open(new BTreeKeyValueDB(fileCollection), true);
        using var tr = db.StartTransaction();
        var table = tr.GetRelation<IGeneratedIterateTable>();
        Seed(table);
        var context = new object();
        var value = new IterateValueOnlyProjection { Context = context };
        var count = 0;
        table.IterateById(1, item =>
        {
            Assert.Same(value, item);
            Assert.Same(context, item.Context);
            Assert.Equal("ignored", item.Payload);
            count++;
        }, value);
        Assert.Equal(3, count);
    }

    [Generate]
    public class IterateOldRow
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong UserId { get; set; }
        [PrimaryKey(3)] public ulong RoleId { get; set; }
        public int Removed { get; set; }
        public string Payload { get; set; } = "";
    }

    [Generate]
    [PersistedName("IterateVersions")]
    public interface IIterateOldTable : IRelation<IterateOldRow>;

    [Generate]
    [PersistedName("IterateVersions")]
    public interface IIterateNewTable : IRelation<IterateRow>
    {
        void IterateById(ulong companyId, Action<IterateValueOnlyProjection> callback, IterateValueOnlyProjection value);
    }

    [Fact]
    public void IterateByIdDeserializesMixedStoredSchemaVersions()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var kvDb = new BTreeKeyValueDB(fileCollection);
        using (var db = new ObjectDB())
        {
            db.Open(kvDb, false);
            using var tr = db.StartTransaction();
            var table = tr.GetRelation<IIterateOldTable>();
            table.Upsert(new() { CompanyId = 1, UserId = 2, RoleId = 1, Removed = 123, Payload = "old" });
            tr.Commit();
        }
        using (var db = new ObjectDB())
        {
            db.Open(kvDb, false);
            using var tr = db.StartTransaction();
            var table = tr.GetRelation<IIterateNewTable>();
            table.Upsert(new() { CompanyId = 1, UserId = 1, RoleId = 1, Payload = "new" });
            table.Upsert(new() { CompanyId = 1, UserId = 3, RoleId = 1, Payload = "new again" });
            tr.Commit();
        }
        using (var db = new ObjectDB())
        {
            db.Open(kvDb, false);
            using var tr = db.StartReadOnlyTransaction();
            var table = tr.GetRelation<IIterateNewTable>();
            var value = new IterateValueOnlyProjection();
            var seen = new List<string>();
            table.IterateById(1, item =>
            {
                Assert.Same(value, item);
                seen.Add(item.Payload);
            }, value);
            Assert.Equal(["new", "old", "new again"], seen);
        }
    }

    static void Seed(IRelation<IterateRow> table)
    {
        table.Upsert(new() { CompanyId = 1, UserId = 10, RoleId = 1, Payload = "ignored" });
        table.Upsert(new() { CompanyId = 1, UserId = 10, RoleId = 2, Payload = "ignored" });
        table.Upsert(new() { CompanyId = 1, UserId = 20, RoleId = 1, Payload = "ignored" });
        table.Upsert(new() { CompanyId = 2, UserId = 30, RoleId = 1, Payload = "other company" });
    }

    static void AssertIteration(Action<Action<IterateProjection>, IterateProjection> iterate)
    {
        var context = new object();
        var value = new IterateProjection { Context = context };
        iterate(item =>
        {
            Assert.Same(value, item);
            Assert.Same(context, item.Context);
            item.SeenInstances.Add(item);
            item.SeenValues.Add((item.UserId, item.RoleId));
        }, value);

        Assert.Equal([(10ul, 1ul), (10ul, 2ul), (20ul, 1ul)], value.SeenValues);
        Assert.Equal(3, value.SeenInstances.Count);
        Assert.All(value.SeenInstances, item => Assert.Same(value, item));
    }

    static Type CreateReflectionRelationInterface(Type? projectionType = null)
    {
        projectionType ??= typeof(IterateProjection);
        var assembly = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("BTDBReflectionIterate" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var module = assembly.DefineDynamicModule("Main");
        var type = module.DefineType("IReflectionIterateTable", TypeAttributes.Public | TypeAttributes.Interface |
                                                               TypeAttributes.Abstract);
        type.AddInterfaceImplementation(typeof(IRelation<IterateRow>));
        var method = type.DefineMethod("IterateById",
            MethodAttributes.Public | MethodAttributes.Abstract | MethodAttributes.Virtual |
            MethodAttributes.NewSlot | MethodAttributes.HideBySig,
            typeof(void), [typeof(ulong), typeof(Action<>).MakeGenericType(projectionType), projectionType]);
        method.DefineParameter(1, ParameterAttributes.None, "companyId");
        method.DefineParameter(2, ParameterAttributes.None, "callback");
        method.DefineParameter(3, ParameterAttributes.None, "value");
        return type.CreateType();
    }
}
