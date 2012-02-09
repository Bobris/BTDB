using System;
using System.Threading.Tasks;

namespace BTDB.KV2DBLayer
{
    public class KeyValue2DB : IKeyValue2DB
    {
        readonly IFileCollection _fileCollection;

        public KeyValue2DB(IFileCollection fileCollection)
        {
            _fileCollection = fileCollection;
        }

        public void Dispose()
        {
        }

        public IKeyValue2DBTransaction StartTransaction()
        {
            throw new NotImplementedException();
        }

        public Task<IKeyValue2DBTransaction> StartWritingTransaction()
        {
            throw new NotImplementedException();
        }
    }
}