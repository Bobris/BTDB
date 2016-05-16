using System.ComponentModel;
using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events
{
    [DataContract]
    public class PageContentWithButton
    {
        [DataMember(Order = 1)]
        [DisplayName("Page Title")]
        public string Title { get; set; }

        [DataMember(Order = 2)]
        [DisplayName("Page Message")]
        public string Message { get; set; }

        [DataMember(Order = 3)]
        public string SendButtonText { get; set; }
    }
}