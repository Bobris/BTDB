using System;

namespace SimpleTester.RelationTest
{
    public class LicenseCancelDb
    {
        public ulong RequestedByUserId { get; set; }
        public ulong ConfirmedByUserId { get; set; }
        public DateTime Date { get; set; }
    }
}