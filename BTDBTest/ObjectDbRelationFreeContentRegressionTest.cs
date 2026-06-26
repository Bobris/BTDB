using System;
using System.Collections.Generic;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbRelationFreeContentRegressionTest : IDisposable
{
    readonly bool _oldUseNoEmit;
    readonly bool _oldUseNoEmitForKeyValue;
    readonly bool _oldUseNoEmitForRelations;
    readonly bool _oldUseNoEmitForDescriptors;
    readonly IKeyValueDB _lowDb;
    readonly IObjectDB _db;

    public ObjectDbRelationFreeContentRegressionTest()
    {
        _oldUseNoEmit = IFieldHandler.UseNoEmit;
        _oldUseNoEmitForKeyValue = IFieldHandler.UseNoEmitForKeyValue;
        _oldUseNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
        _oldUseNoEmitForDescriptors = IFieldHandler.UseNoEmitForDescriptors;
        IFieldHandler.UseNoEmit = false;
        IFieldHandler.UseNoEmitForKeyValue = false;
        IFieldHandler.UseNoEmitForRelations = false;
        IFieldHandler.UseNoEmitForDescriptors = false;
        ObjectDB.ResetAllMetadataCaches();

        _lowDb = new InMemoryKeyValueDB();
        _db = new ObjectDB();
        _db.Open(_lowDb, false, new DBOptions().WithoutAutoRegistration());
    }

    public void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
        IFieldHandler.UseNoEmit = _oldUseNoEmit;
        IFieldHandler.UseNoEmitForKeyValue = _oldUseNoEmitForKeyValue;
        IFieldHandler.UseNoEmitForRelations = _oldUseNoEmitForRelations;
        IFieldHandler.UseNoEmitForDescriptors = _oldUseNoEmitForDescriptors;
        ObjectDB.ResetAllMetadataCaches();
    }

    public class SampleConfigurationDb
    {
        [PrimaryKey(1)] public ulong LocalOfficeId { get; set; }
        [PrimaryKey(2)] public ulong ItemId { get; set; }
        public SampleConfigurationReportListDto ReportList { get; set; }
        public string Name { get; set; }
        public IDictionary<ulong, bool> UsedInCompanyIds { get; set; }
    }

    public class SampleConfigurationReportListDto
    {
        public IList<SampleConfigurationReportDto> Items { get; set; }
    }

    public class SampleConfigurationReportDto : ResponseWithError
    {
        public string Name { get; set; }
    }

    public class ResponseWithError
    {
        public ErrorInfo Error { get; set; }
    }

    public class ErrorInfo : ErrorInfoBase
    {
        public ErrorInfoType ErrorType { get; set; }
        public string WhatHappenedKey { get; set; }
        public string HintKey { get; set; }
        public bool UseOriginalError { get; set; }
        public IDictionary<string, IList<ErrorInfoBase>> PropertyErrors { get; set; }
    }

    public enum ErrorInfoType
    {
        General,
        Validation,
        Warning,
        ValidationWithError,
        Info
    }

    public class ErrorInfoBase
    {
        public string ErrorKey { get; set; }
        public IList<string> Parameters { get; set; }
        public IList<int> LocalizableParametersIds { get; set; }
        public IDictionary<int, IDictionary<string, string>> ParametersLocalizations { get; set; }
        public IDictionary<string, string> ErrorLocalizations { get; set; }
        public string RuleInvalidData { get; set; }
    }

    public interface ISampleConfigurationTable : IRelation<SampleConfigurationDb>
    {
        void Insert(SampleConfigurationDb configurationDb);
        bool RemoveById(ulong localOfficeId, ulong itemId);
    }

    public class ReleaseLike
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public string Version { get; set; }
        public string Label { get; set; }
        public bool Inactive { get; set; }
        public uint Build { get; set; }
        public IList<PackageLike> Packages { get; set; }
        public string Description { get; set; }
        public IDictionary<int, string> GroupsModelMapping { get; set; }
        public IList<int> LegacyModels { get; set; }
        public ushort LicensedModulesBlockId { get; set; }
    }

    public class PackageLike
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public bool IsMandatory { get; set; }
        public uint Version { get; set; }
        public bool IsActive { get; set; }
        public IList<string> Applications { get; set; }
        public IList<CounterLike> Counters { get; set; }
        public IList<string> Options { get; set; }
        public IList<string> OtherValues { get; set; }
        public IList<string> LicensedModules { get; set; }
        public ISet<string> LicensedAsAServices { get; set; }
        public ISet<string> LicensedCloudModules { get; set; }
        public bool IsDisabled { get; set; }
        public IList<string> Requires { get; set; }
        public IList<string> ExcludedFromSelection { get; set; }
        public string Info { get; set; }
        public IList<int> GroupsToShown { get; set; }
        public bool Internal { get; set; }
    }

    public class CounterLike
    {
        public string Name { get; set; }
        public short CounterId { get; set; }
        public long VolumeAmount { get; set; }
        public IList<long> VolumeAmounts { get; set; }
        public string VolumeUnitType { get; set; }
        public IList<string> VolumeUnitTypes { get; set; }
        public CounterTypeLike Type { get; set; }
        public CounterSubTypeLike SubType { get; set; }
        public bool DisplayChart { get; set; }
        public bool HideSettingsMode { get; set; }
        public int Step { get; set; }
        public ulong MaxValue { get; set; }
        public ulong MinValue { get; set; }
        public bool DoNotShowThreshold { get; set; }
        public short ClientCounterId { get; set; }
        public IList<short> AddToCounters { get; set; }
        public IList<short> AddToFirtsActiveCounter { get; set; }
        public bool HideConsumptionForCustomer { get; set; }
        public bool IsOutput { get; set; }
        public bool HasOutputs { get; set; }
        public string Application { get; set; }
        public bool NonProdAlertDefault { get; set; }
    }

    public enum CounterTypeLike
    {
        Total,
        Maximal,
        Merged,
        TotalPerDay
    }

    public enum CounterSubTypeLike
    {
        Period,
        Current
    }

    public interface IReleaseLikeTable : IRelation<ReleaseLike>
    {
        void Insert(ReleaseLike release);
        ReleaseLike FindById(ulong id);
        void Update(ReleaseLike release);
    }

    [Fact]
    public void RemoveByIdFreesSampleConfigurationWithNestedDictionaries()
    {
        Func<IObjectDBTransaction, ISampleConfigurationTable> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ISampleConfigurationTable>("SampleConfiguration");
            var table = creator(tr);
            table.Insert(new SampleConfigurationDb
            {
                LocalOfficeId = 1,
                ItemId = 2,
                ReportList = new SampleConfigurationReportListDto
                {
                    Items =
                    [
                        new SampleConfigurationReportDto
                        {
                            Name = "Report",
                            Error = CreateErrorInfo()
                        }
                    ]
                },
                Name = "Preloaded configuration",
                UsedInCompanyIds = new Dictionary<ulong, bool> { [1] = true, [2] = false }
            });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var table = creator(tr);
            Assert.True(table.RemoveById(1, 2));
            tr.Commit();
        }
    }

    [Fact]
    public void RemoveByIdFreesSampleConfigurationWithSharedInlineObject()
    {
        Func<IObjectDBTransaction, ISampleConfigurationTable> creator;
        var sharedReport = new SampleConfigurationReportDto
        {
            Name = "Shared",
            Error = CreateErrorInfo()
        };

        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ISampleConfigurationTable>("SampleConfiguration");
            var table = creator(tr);
            table.Insert(new SampleConfigurationDb
            {
                LocalOfficeId = 1,
                ItemId = 2,
                ReportList = new SampleConfigurationReportListDto
                {
                    Items = [sharedReport, sharedReport]
                },
                Name = "Preloaded configuration",
                UsedInCompanyIds = new Dictionary<ulong, bool> { [1] = true, [2] = false }
            });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var table = creator(tr);
            Assert.True(table.RemoveById(1, 2));
            tr.Commit();
        }
    }

    [Fact]
    public void UpdateFreesReleaseLikeNestedPackageCounters()
    {
        Func<IObjectDBTransaction, IReleaseLikeTable> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<IReleaseLikeTable>("ReleaseLike");
            var table = creator(tr);
            table.Insert(new ReleaseLike
            {
                Id = 1,
                Packages =
                [
                    new PackageLike
                    {
                        Id = "PKG1",
                        Name = "Package 1",
                        Counters =
                        [
                            new CounterLike { CounterId = 1, Name = "a" },
                            new CounterLike { CounterId = 4, Name = "aa" },
                            new CounterLike { CounterId = 6, Name = "ab" },
                            new CounterLike { CounterId = 7, Name = "ab" }
                        ]
                    },
                    new PackageLike
                    {
                        Id = "PKG2",
                        Name = "Package 2",
                        Counters =
                        [
                            new CounterLike { CounterId = 2, Name = "b" },
                            new CounterLike { CounterId = 1, Name = "bb" },
                            new CounterLike { CounterId = 5, DoNotShowThreshold = true },
                            new CounterLike { CounterId = 8 }
                        ]
                    }
                ]
            });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var release = creator(tr).FindById(1);
            var counter = release.Packages[1].Counters[1];
            counter.Type = CounterTypeLike.Maximal;
            counter.SubType = CounterSubTypeLike.Current;
            creator(tr).Update(release);
            tr.Commit();
        }
    }

    static ErrorInfo CreateErrorInfo()
    {
        return new ErrorInfo
        {
            ErrorType = ErrorInfoType.Validation,
            ErrorKey = "ErrorKey",
            Parameters = ["parameter"],
            LocalizableParametersIds = [1],
            ParametersLocalizations = new Dictionary<int, IDictionary<string, string>>
            {
                [1] = new Dictionary<string, string> { ["en"] = "message" }
            },
            ErrorLocalizations = new Dictionary<string, string> { ["en"] = "localized" },
            RuleInvalidData = "invalid",
            WhatHappenedKey = "WhatHappened",
            HintKey = "Hint",
            UseOriginalError = true,
            PropertyErrors = new Dictionary<string, IList<ErrorInfoBase>>
            {
                ["Name"] =
                [
                    new ErrorInfoBase
                    {
                        ErrorKey = "Nested",
                        Parameters = ["nested"],
                        ParametersLocalizations = new Dictionary<int, IDictionary<string, string>>
                        {
                            [2] = new Dictionary<string, string> { ["en"] = "nested message" }
                        },
                        ErrorLocalizations = new Dictionary<string, string> { ["en"] = "nested localized" }
                    }
                ]
            }
        };
    }
}
