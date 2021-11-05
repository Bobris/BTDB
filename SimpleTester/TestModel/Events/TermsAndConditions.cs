
using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events;

[DataContract]
public class TermsAndConditions
{
    [DataMember(Order = 1)]
    public bool Use { get; set; }

    [DataMember(Order = 2)]
    public string? Message { get; set; }
}
