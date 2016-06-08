using System;

namespace SimpleTester.RelationTest
{
    public class BlobLocation
    {
        public string Prefix { get; set; }
        public string Name { get; set; }
    }

    public class LicenseFileDb
    {
        public string FileName { get; set; }
        public BlobLocation Location { get; set; }
        public DateTime GeneratedDate { get; set; }
    }
}