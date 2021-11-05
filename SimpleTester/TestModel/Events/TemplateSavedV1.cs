using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events;

[DataContract]
public class TemplateSavedV1 : ActionFinishedBase<string>
{
    [DataMember(Order = 1)]
    public ulong CompanyId { get; set; }
    [DataMember(Order = 2)]
    public string? SessionId { get; set; }
    [DataMember(Order = 3)]
    public string? WindowId { get; set; }
    [DataMember(Order = 4)]
    public ulong TemplateId { get; set; }
    [DataMember(Order = 5)]
    public ulong OldTemplateId { get; set; }
    [DataMember(Order = 6)]
    public string? Name { get; set; }
    [DataMember(Order = 7)]
    public TemplateConfiguration? Configuration { get; set; }
    [DataMember(Order = 8)]
    public bool IsCreating { get; set; }
    [DataMember(Order = 9)]
    public ulong LogoId { get; set; }
    [DataMember(Order = 10)]
    public ulong SavedFromJobId { get; set; }
    [DataMember(Order = 11)]
    public bool ForceSave { get; set; }
}
