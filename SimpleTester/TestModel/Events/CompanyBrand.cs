
using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events
{
    [DataContract]
    public class CompanyBrand
    {
        [DataMember(Order = 1)]
        public string? FontFamily { get; set; }

        [DataMember(Order = 2)]
        public FontSettings? HeaderFont { get; set; }

        [DataMember(Order = 3)]
        public FontSettings? TitleFont { get; set; }

        [DataMember(Order = 4)]
        public FontSettings? TextFont { get; set; }

        [DataMember(Order = 5)]
        public string? HeaderBackgroundColor { get; set; }

        [DataMember(Order = 6)]
        public string? ContentBackgroundColor { get; set; }

        [DataMember(Order = 7)]
        public string? ContentButtonTextColor { get; set; }

        [DataMember(Order = 8)]
        public string? ContentButtonColor { get; set; }
    }
}
