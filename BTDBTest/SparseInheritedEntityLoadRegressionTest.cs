using System;
using System.Collections.Generic;
using BTDB;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class SparseInheritedEntityLoadRegressionTest : ObjectDbTestBase
{
    public SparseInheritedEntityLoadRegressionTest(ITestOutputHelper output) : base(output)
    {
    }

    [Generate]
    public class BlobLocation
    {
        public string Prefix { get; set; } = null!;
        public string Name { get; set; } = null!;
        public ulong EncryptKeyId { get; set; }
        public uint EncryptMethod { get; set; }
    }

    [Generate]
    public class BlobLocationWithSize : BlobLocation
    {
        public long WrittenSize { get; set; }
    }

    public enum FileType
    {
        Jpg,
        Png
    }

    [Generate]
    public abstract class UploadDataWithSizeBase
    {
        public BlobLocationWithSize Location { get; set; } = null!;
        public string FileName { get; set; } = null!;
        public string UploadGuid { get; set; } = null!;
        public string SessionId { get; set; } = null!;
        public DateTime UploadDate { get; set; }
    }

    [Generate]
    public abstract class UploadDataWithTypeBase : UploadDataWithSizeBase
    {
        public FileType Type { get; set; }
    }

    public enum ImageType
    {
        Custom,
        Predefine
    }

    [Generate]
    [GenerateFor(typeof(ISharedImageFileTableBase))]
    public class SharedImageFile : UploadDataWithTypeBase, ICompanyRecord
    {
        [PrimaryKey(1)]
        [SecondaryKey("CompanyId")]
        public ulong CompanyId { get; set; }

        [PrimaryKey(2)]
        public ulong Id { get; set; }

        public new BlobLocationWithSize Location
        {
            get => base.Location;
            set => base.Location = value;
        }

        public BlobLocationWithSize ThumbnailLocation { get; set; } = null!;
        public int Width { get; set; }
        public int Height { get; set; }
        public ImageType ImageType { get; set; }
    }

    public interface ICompanyRecord
    {
        ulong CompanyId { get; }
    }

    public interface ICovariantCompanyItemTableBase<out T> : ICovariantRelation<T> where T : class, ICompanyRecord
    {
        IEnumerable<T> FindById(ulong companyId);
        int RemoveById(ulong companyId);
    }

    public interface ICompanyItemTableBase<T> : ICovariantCompanyItemTableBase<T>, IRelation<T>
        where T : class, ICompanyRecord
    {
    }

    [PersistedName("Gmc.Cloud.Cjm.Data.ISharedImageFileTable")]
    public interface ISharedImageFileTableBase : ICompanyItemTableBase<SharedImageFile>
    {
        SharedImageFile FindById(ulong companyId, ulong id);
        void RemoveById(ulong companyId, ulong id);
        SharedImageFile? FindByIdOrDefault(ulong companyId, ulong id);
        IEnumerable<SharedImageFile> FindByCompanyId(ulong companyId);
    }

    public sealed class SharedImageFileTable
    {
        readonly ISharedImageFileTableBase _sharedImageTable;

        public SharedImageFileTable(IObjectDBTransaction tr)
        {
            _sharedImageTable = tr.GetRelation<ISharedImageFileTableBase>();
        }

        public bool Upsert(SharedImageFile item)
        {
            return _sharedImageTable.Upsert(item);
        }

        public SharedImageFile? FindByIdOrDefault(ulong companyId, ulong id)
        {
            return _sharedImageTable.FindByIdOrDefault(companyId, id);
        }
    }

    [Fact]
    public void SparseInheritedEntityCanBeReadBack()
    {
        using (var tr = _db.StartTransaction())
        {
            var table = new SharedImageFileTable(tr);
            table.Upsert(new SharedImageFile
            {
                CompanyId = 1,
                Id = 1200
            });
            tr.Commit();
        }

        using (var tr = _db.StartReadOnlyTransaction())
        {
            var table = new SharedImageFileTable(tr);
            var item = table.FindByIdOrDefault(1, 1200);
            Assert.NotNull(item);
            Assert.Equal((ulong)1, item.CompanyId);
            Assert.Equal((ulong)1200, item.Id);
        }
    }
}