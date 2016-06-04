using System;
using BTDB.ODBLayer;

namespace SimpleTester.RelationTest
{
    [StoredInline]
    public class LicenseOrderDb
    {
        public OrderType Type { get; set; }
        public DateTime Date { get; set; }
        public ulong UserId { get; set; }
    }
}