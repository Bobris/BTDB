using BTDB.Buffer;
using BTDB.EventStore2Layer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SimpleTester
{
    public class EventLayer2TestWithALotOfClasses
    {
        public class MetadataStream
        {
            public int Push(ByteBuffer buffer)
            {
                Queue.Add(buffer.ToAsyncSafe());
                return Queue.Count - 1;
            }

            public List<ByteBuffer> Queue { get; } = new List<ByteBuffer>();
        }

        public class SerializerTester
        {
            public SerializerTester(MetadataStream metadataStream, Random random)
            {
                _metadata = metadataStream;
                _rand = random;
            }

            MetadataStream _metadata;
            EventSerializer _serializer = new EventSerializer();
            Random _rand;
            object[] _instances = InstantiateALotOfClasses.Gen().ToArray();
            int _metaIndex;
            int _saving;
            bool _metaSaved;
            int _metaSavedIndex;

            internal void DoSomething()
            {
                switch (_rand.Next(3))
                {
                    case 0:
                        while (_metaIndex < _metadata.Queue.Count)
                        {
                            _serializer.ProcessMetadataLog(_metadata.Queue[_metaIndex++]);
                        }
                        break;
                    case 1:
                        if (_metaSaved && _metaIndex <= _metaSavedIndex) break;
                        var buf = _serializer.Serialize(out var hasMetaData, _instances[_saving]);
                        if (hasMetaData)
                        {
                            if (!_metaSaved)
                            {
                                _metaSavedIndex = _metadata.Push(buf);
                                _metaSaved = true;
                            }
                            else
                            {
                                throw new Exception("Should not need to save another metadata");
                            }
                        }
                        else
                        {
                            _saving = _rand.Next(_instances.Length);
                            _metaSaved = false;
                        }
                        break;
                    case 2:
                        // wait
                        break;
                }
            }
        }

        public void Run()
        {
            var rand = new Random(100);
            var m = new MetadataStream();
            var s1 = new SerializerTester(m, rand);
            var s2 = new SerializerTester(m, rand);
            var s3 = new SerializerTester(m, rand);
            for (int i = 0; i < 100000; i++)
            {
                switch (rand.Next(3))
                {
                    case 0: s1.DoSomething(); break;
                    case 1: s2.DoSomething(); break;
                    case 2: s3.DoSomething(); break;
                }
            }
        }
    }
}
