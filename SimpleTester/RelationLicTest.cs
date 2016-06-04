using System;
using System.Collections.Generic;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using SimpleTester.RelationTest;

namespace SimpleTester
{
    public class RelationLicTest
    {
       
        static ObjectDB CreateInMemoryDb()
        {
            var lowDb = new InMemoryKeyValueDB();
            var db = new ObjectDB();
            db.Open(lowDb, true);
            return db;
        }

        static LicenseDb CreateLic(ulong id)
        {
            return new LicenseDb
            {
                ItemId = id,
                Cancel = new LicenseCancelDb { ConfirmedByUserId = 1},
                CompanyId = 33,
                CustomerId = 22,
                EndDate = DateTime.UtcNow,
                Features = new List<LicenseFeatureDb> {new LicenseFeatureDb { FeatureType = FeatureType.Concurrent} },
                Order = new LicenseOrderDb(),
                LastUpdate = DateTime.UtcNow,
                LicenseFile = new LicenseFileDb { Location = new BlobLocation { Name = "test"} },
                MachineId = "123",
                Product = Product.Big500,
                Release = new LicenseReleaseDb { Reason = "for me"},
                SerialNumber = "333",
                Status = LicenseStatus.Draft
            };
        }

        public void Run()
        {
            Func<IObjectDBTransaction, ILicenseTable> _creator;

            using (var db = CreateInMemoryDb())
            {
                using (var tr = db.StartTransaction())
                {
                    _creator = tr.InitRelation<ILicenseTable>("License");
                    var licRel = _creator(tr);
                    licRel.Insert(CreateLic(11));
                    licRel.Insert(CreateLic(12));
                    var lic = licRel.FindByIdOrDefault(12);
                    lic = licRel.FindByIdOrDefault(11);
                    tr.Commit();
                }
                using (var tr = db.StartTransaction())
                {
                    var licRel = _creator(tr);
                    licRel.RemoveById(11);
                    var en = licRel.FindByCompanyId(33);
                    var lic1 = en.Current;
                    if (lic1.ItemId != 12) throw new Exception("Bug");
                    var lic = licRel.FindByIdOrDefault(11);
                }
            }
        }
    }
}