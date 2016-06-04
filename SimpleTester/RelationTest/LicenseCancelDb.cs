using System;
using BTDB.ODBLayer;

namespace SimpleTester.RelationTest
{
    [StoredInline]
    public class LicenseCancelDb
    {
        public ulong RequestedByUserId { get; set; }
        public ulong ConfirmedByUserId { get; set; }
        public DateTime Date { get; set; }
    }
}