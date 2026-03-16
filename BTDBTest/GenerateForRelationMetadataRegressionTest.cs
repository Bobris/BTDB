using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class GenerateForRelationMetadataRegressionTest : ObjectDbTestBase
{
    public GenerateForRelationMetadataRegressionTest(ITestOutputHelper output) : base(output)
    {
    }

    public abstract class File
    {
        [PrimaryKey]
        public ulong Id { get; set; }

        public string FileName { get; set; }
    }

    [GenerateFor(typeof(IFileTable<DownloadSectionFile>))]
    public class DownloadSectionFile : File
    {
        public string UploadGuid { get; set; }
    }

    public interface IFileTable<TFile> : IRelation<TFile> where TFile : File
    {
        void Insert(TFile file);
        TFile FindByIdOrDefault(ulong id);
    }

    [Fact]
    public void GenerateForOnRelationShouldProvideMetadataForDerivedEntity()
    {
        using var tr = _db.StartTransaction();
        var relation = tr.GetRelation<IFileTable<DownloadSectionFile>>();
        Assert.NotNull(relation);
    }
}