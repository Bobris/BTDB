using BTDB.EventStore2Layer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SimpleTester
{
    public class EventLayer2TestWithALotOfClasses
    {
        public void Run()
        {
            var instances = InstantiateALotOfClasses.Gen().ToArray();
            var serializer = new EventSerializer();
            for (var i = 0; i < instances.Length; i++)
            {
                var buf = serializer.Serialize(out bool hasMetadata, instances[i]).ToAsyncSafe();
                if (!hasMetadata)
                    throw new Exception("It should not have known this type");
                serializer.ProcessMetadataLog(buf);
                buf = serializer.Serialize(out hasMetadata, instances[i]).ToAsyncSafe();
                if (hasMetadata)
                    throw new Exception("It should have known this type");
                for (var j = 0; j < i; j++)
                {
                    buf = serializer.Serialize(out hasMetadata, instances[j]).ToAsyncSafe();
                    if (hasMetadata)
                        throw new Exception("It should have known this type");
                }
            }
        }
    }
}
