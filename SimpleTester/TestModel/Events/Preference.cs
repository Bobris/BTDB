using System.Collections.Generic;
using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events
{
    [DataContract]
    public class Preference
    {
        [DataMember(Order = 1)]
        public string Heading { get; set; }
        [DataMember(Order = 2)]
        public PageContentWithButton Content { get; set; }

        [DataMember(Order = 3)]
        public List<Option> Options { get; set; }

        [DataMember(Order = 4)]
        public IDictionary<string, Option> OptionsLookup { get; set; }

        [DataMember(Order = 5)]
        public PageContent Confirmation { get; set; }

        [DataMember(Order = 6)]
        public TermsAndConditions TermsAndConditions { get; set; } 
    }
}