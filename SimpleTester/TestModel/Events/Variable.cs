
using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events;

[DataContract]
public class Variable
{
    [DataMember(Order = 1)]
    public string? Name { get; set; }

    [DataMember(Order = 2)]
    public string? DefaultValue { get; set; }

    public Variable() { }

    public Variable(string name, string? defaultValue = null)
    {
        Name = name;
        DefaultValue = defaultValue;
    }
}
