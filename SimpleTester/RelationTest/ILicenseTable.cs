using System.Collections.Generic;

namespace SimpleTester.RelationTest
{
    public interface ILicenseTable
    {
        void Insert(LicenseDb license);
        void Update(LicenseDb person);
        LicenseDb FindByIdOrDefault(ulong itemId);
        IEnumerator<LicenseDb> GetEnumerator();
        IEnumerator<LicenseDb> FindByCompanyId(ulong companyId);
        void RemoveById(ulong itemId);
    }
}