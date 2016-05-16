using System.ComponentModel;
using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events
{
    [DataContract]
    public class PageContent
    {
        [DisplayName("Page Title")]
        [DataMember(Order = 1)]
        public string Title { get; set; }

        [DisplayName("Page Message")]
        [DataMember(Order = 2)]
        public string Message { get; set; }
    }
}