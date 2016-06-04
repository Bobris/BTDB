using System;
using BTDB.ODBLayer;

namespace SimpleTester.RelationTest
{
    [StoredInline]
    public class LicenseReleaseDb
    {
        public DateTime Date { get; set; }
        public string Reason { get; set; }
    }
}