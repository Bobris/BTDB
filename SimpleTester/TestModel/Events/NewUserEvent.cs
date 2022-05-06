using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events;

[DataContract]
public class NewUserEvent : Event
{
    [DataMember(Order = 1)]
    public string? Name { get; set; }

    [DataMember(Order = 2)]
    public byte[]? Password { get; set; }
}
