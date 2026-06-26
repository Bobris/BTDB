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
