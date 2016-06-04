using System;
using System.Collections.Generic;
using BTDB.ODBLayer;

namespace SimpleTester.RelationTest
{
    public class LicenseDb
    {
        [PrimaryKey(1)]
        public ulong ItemId { get; set; }

        [SecondaryKey(nameof(CompanyId))]
        public ulong CompanyId { get; set; }

        public LicenseStatus Status { get; set; }
        public string SerialNumber { get; set; }
        public string MachineId { get; set; }
        public bool Maintenance { get; set; }
        public DateTime LastUpdate { get; set; }
        public DateTime StartDate { get; set; } //??
        public DateTime EndDate { get; set; }
        public IList<LicenseFeatureDb> Features { get; set; }
        public ulong CustomerId { get; set; }
        public Product Product { get; set; }

        public LicenseFileDb LicenseFile { get; set; }
        public LicenseOrderDb Order { get; set; }
        public LicenseReleaseDb Release { get; set; }
        public LicenseCancelDb Cancel { get; set; }
    }
}
