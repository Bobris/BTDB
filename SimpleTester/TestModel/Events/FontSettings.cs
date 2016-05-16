
using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events
{
    [DataContract]
    public class FontSettings
    {
        [DataMember(Order = 1)]
        public double Size { get; set; }

        [DataMember(Order = 2)]
        public string Style { get; set; }
        
        [DataMember(Order = 3)]
        public string Color { get; set; }
    }
}
