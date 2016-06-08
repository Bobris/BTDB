using System;

namespace SimpleTester.RelationTest
{
    public class LicenseOrderDb
    {
        public OrderType Type { get; set; }
        public DateTime Date { get; set; }
        public ulong UserId { get; set; }
    }
}