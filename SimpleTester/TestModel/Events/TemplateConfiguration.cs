using System.Collections.Generic;
using System.Runtime.Serialization;
using ProtoBuf;

namespace SimpleTester.TestModel.Events
{
    [DataContract]
    public class TemplateConfiguration
    {
        [DataMember(Order = 1)]
        public Language Language { get; set; }

        [DataMember(Order = 2)]
        public IList<Variable>? Variables { get; set; }

        [DataMember(Order = 3)]
        public Invitation? Invitation { get; set; }

        [DataMember(Order = 4)]
        public Preference? Preference { get; set; }

        [DataMember(Order = 5)]
        public CompanyBrand? CompanyBrand { get; set; }

        [DataMember(Order = 6)]
        public Dictionary<string, CompanyBrand>? DifferentCompanyBrands { get; set; }

        [ProtoMember(7)] // Dynamic Object not supported
        public object? ExtraData { get; set; }

        [DataMember(Order = 8)]
        public Languages Languages { get; set; }
    }
}
