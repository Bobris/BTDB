
using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events
{
    [DataContract]
    public class Invitation
    {
        [DataMember(Order = 1)]
        public string? SenderName { get; set; }

        [DataMember(Order = 2)]
        public string? SenderEmail { get; set; }

        [DataMember(Order = 3)]
        public string? Subject { get; set; }

        [DataMember(Order = 4)]
        public string? Content { get; set; }

        [DataMember(Order = 5)]
        public byte[]? Attachment { get; set; }
    }
}
