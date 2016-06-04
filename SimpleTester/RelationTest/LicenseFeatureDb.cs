using System;
using BTDB.ODBLayer;

namespace SimpleTester.RelationTest
{

    public enum EvidenceCode
    {
        One,
        Two,
        Three
    }

    [StoredInline]
    public class LicenseFeatureDb
    {
        public EvidenceCode EvidenceCode { get; set; }
        public DateTime EndDate { get; set; }
        public FeatureType FeatureType { get; set; }
        public uint UsageLimit { get; set; }
        public string CustomAttribute { get; set; }
    }
}