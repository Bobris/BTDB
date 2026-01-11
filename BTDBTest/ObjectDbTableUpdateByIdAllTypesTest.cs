using System;
using System.Linq;
using System.Net;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.ODBLayer;
using Microsoft.Extensions.Primitives;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbTableUpdateByIdAllTypesTest : ObjectDbTestBase
{
    public ObjectDbTableUpdateByIdAllTypesTest(ITestOutputHelper output) : base(output)
    {
    }

    public class AllTypes
    {
        [PrimaryKey(1)] public ulong TenantId { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }

        public string Name { get; set; } = "";
        public bool BoolField { get; set; }
        public byte ByteField { get; set; }
        public sbyte SByteField { get; set; }
        public short ShortField { get; set; }
        public int IntField { get; set; }
        public long LongField { get; set; }
        public ushort UShortField { get; set; }
        public uint UIntField { get; set; }
        public ulong ULongField { get; set; }
        public DateTime DateTimeField { get; set; }
        public DateTimeOffset DateTimeOffsetField { get; set; }
        public TimeSpan TimeSpanField { get; set; }
        public Guid GuidField { get; set; }
        public decimal DecimalField { get; set; }
        public IPAddress IPAddressField { get; set; } = IPAddress.Loopback;
        public byte[] BytesField { get; set; } = Array.Empty<byte>();
        public ByteBuffer ByteBufferField { get; set; }
        public ReadOnlyMemory<byte> ReadOnlyMemoryField { get; set; }
        public Version VersionField { get; set; } = new Version(0, 0);
        public StringValues StringValuesField { get; set; }
        public EncryptedString Secret { get; set; }
    }

    public interface IAllTypesTable : IRelation<AllTypes>
    {
        bool UpdateByIdAll(ulong tenantId, ulong id, string name, bool boolField, byte byteField, sbyte sByteField,
            short shortField, int intField, long longField, ushort uShortField, uint uIntField, ulong uLongField,
            DateTime dateTimeField, DateTimeOffset dateTimeOffsetField, TimeSpan timeSpanField, Guid guidField,
            byte[] bytesField, ByteBuffer byteBufferField,
            ReadOnlyMemory<byte> readOnlyMemoryField, StringValues stringValuesField,
            EncryptedString secret);

        bool UpdateByIdName(ulong tenantId, ulong id, string name);
    }

    [Fact]
    public void UpdateByIdAllTypesWorks()
    {
        var initialBytes = new byte[] { 1, 2, 3 };
        var initialMemory = new ReadOnlyMemory<byte>(new byte[] { 4, 5, 6 });
        var initialValues = new StringValues(new[] { "a", "b" });

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IAllTypesTable>();
        table.Upsert(new AllTypes
        {
            TenantId = 1,
            Id = 10,
            Name = "initial",
            BoolField = true,
            ByteField = 1,
            SByteField = -2,
            ShortField = -3,
            IntField = 4,
            LongField = -5,
            UShortField = 6,
            UIntField = 7,
            ULongField = 8,
            DateTimeField = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            DateTimeOffsetField = new DateTimeOffset(2020, 2, 3, 4, 5, 6, TimeSpan.Zero),
            TimeSpanField = TimeSpan.FromMinutes(7),
            GuidField = new Guid("11111111-2222-3333-4444-555555555555"),
            DecimalField = 12.34m,
            IPAddressField = IPAddress.Parse("127.0.0.1"),
            BytesField = initialBytes,
            ByteBufferField = ByteBuffer.NewAsync(new byte[] { 9, 10 }),
            ReadOnlyMemoryField = initialMemory,
            VersionField = new Version(1, 2, 3, 4),
            StringValuesField = initialValues,
            Secret = "alpha"
        });

        var updatedBytes = new byte[] { 11, 12 };
        var updatedMemory = new ReadOnlyMemory<byte>(new byte[] { 13, 14, 15, 16 });
        var updatedValues = new StringValues(new[] { "c", "d", "e" });

        Assert.True(table.UpdateByIdAll(1, 10, "updated", false, 21, -22, -23, 24, -25, 26, 27, 28,
            new DateTime(2021, 2, 3, 4, 5, 6, DateTimeKind.Utc),
            new DateTimeOffset(2021, 3, 4, 5, 6, 7, TimeSpan.FromHours(1)),
            TimeSpan.FromSeconds(8),
            new Guid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            updatedBytes,
            ByteBuffer.NewAsync(new byte[] { 17, 18, 19 }),
            updatedMemory,
            updatedValues,
            "beta"));

        var updated = table.First();
        Assert.Equal("updated", updated.Name);
        Assert.False(updated.BoolField);
        Assert.Equal((byte)21, updated.ByteField);
        Assert.Equal((sbyte)-22, updated.SByteField);
        Assert.Equal((short)-23, updated.ShortField);
        Assert.Equal(24, updated.IntField);
        Assert.Equal(-25, updated.LongField);
        Assert.Equal((ushort)26, updated.UShortField);
        Assert.Equal(27u, updated.UIntField);
        Assert.Equal(28ul, updated.ULongField);
        Assert.Equal(new DateTime(2021, 2, 3, 4, 5, 6, DateTimeKind.Utc), updated.DateTimeField);
        Assert.Equal(new DateTimeOffset(2021, 3, 4, 5, 6, 7, TimeSpan.FromHours(1)), updated.DateTimeOffsetField);
        Assert.Equal(TimeSpan.FromSeconds(8), updated.TimeSpanField);
        Assert.Equal(new Guid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), updated.GuidField);
        Assert.Equal(12.34m, updated.DecimalField);
        Assert.Equal(IPAddress.Parse("127.0.0.1"), updated.IPAddressField);
        Assert.Equal(updatedBytes, updated.BytesField);
        Assert.Equal(ByteBuffer.NewAsync(new byte[] { 17, 18, 19 }), updated.ByteBufferField);
        Assert.Equal(updatedMemory.ToArray(), updated.ReadOnlyMemoryField.ToArray());
        Assert.Equal(new Version(1, 2, 3, 4), updated.VersionField);
        Assert.Equal(updatedValues.ToArray(), updated.StringValuesField.ToArray());
        Assert.Equal("beta", updated.Secret.Secret);

        Assert.True(table.UpdateByIdName(1, 10, "renamed"));

        var renamed = table.First();
        Assert.Equal("renamed", renamed.Name);
        Assert.False(renamed.BoolField);
        Assert.Equal((byte)21, renamed.ByteField);
        Assert.Equal((sbyte)-22, renamed.SByteField);
        Assert.Equal((short)-23, renamed.ShortField);
        Assert.Equal(24, renamed.IntField);
        Assert.Equal(-25, renamed.LongField);
        Assert.Equal((ushort)26, renamed.UShortField);
        Assert.Equal(27u, renamed.UIntField);
        Assert.Equal(28ul, renamed.ULongField);
        Assert.Equal(new DateTime(2021, 2, 3, 4, 5, 6, DateTimeKind.Utc), renamed.DateTimeField);
        Assert.Equal(new DateTimeOffset(2021, 3, 4, 5, 6, 7, TimeSpan.FromHours(1)), renamed.DateTimeOffsetField);
        Assert.Equal(TimeSpan.FromSeconds(8), renamed.TimeSpanField);
        Assert.Equal(new Guid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), renamed.GuidField);
        Assert.Equal(12.34m, renamed.DecimalField);
        Assert.Equal(IPAddress.Parse("127.0.0.1"), renamed.IPAddressField);
        Assert.Equal(updatedBytes, renamed.BytesField);
        Assert.Equal(ByteBuffer.NewAsync(new byte[] { 17, 18, 19 }), renamed.ByteBufferField);
        Assert.Equal(updatedMemory.ToArray(), renamed.ReadOnlyMemoryField.ToArray());
        Assert.Equal(new Version(1, 2, 3, 4), renamed.VersionField);
        Assert.Equal(updatedValues.ToArray(), renamed.StringValuesField.ToArray());
        Assert.Equal("beta", renamed.Secret.Secret);
    }
}
