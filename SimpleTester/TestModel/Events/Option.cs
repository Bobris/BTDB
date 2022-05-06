
using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events;

[DataContract]
public class Option
{
    [DataMember(Order = 1)]
    public string? Value { get; set; }
}
