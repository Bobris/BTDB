using System;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using BTDB.Buffer;
using BTDB.EventStore2Layer;
using BTDB.StreamLayer;
using FluentAssertions;
using ProtoBuf.Meta;
using SimpleTester.TestModel;
using SimpleTester.TestModel.Events;

namespace SimpleTester
{
    public class ColumnOrderFirst : IColumn
    {
        readonly IColumn _parent;

        public ColumnOrderFirst(IColumn parent)
        {
            _parent = parent;
        }

        public string GetValue(Summary summary, Benchmark benchmark)
        {
            return _parent.GetValue(summary, benchmark);
        }

        public bool IsAvailable(Summary summary)
        {
            return _parent.IsAvailable(summary);
        }

        public string ColumnName => _parent.ColumnName;
        public bool AlwaysShow => _parent.AlwaysShow;
        public ColumnCategory Category => ColumnCategory.Job;
    }

    public class ByteSizeColumn : IColumn
    {
        public string GetValue(Summary summary, Benchmark benchmark)
        {
            var target = benchmark.Target;
            var instance = Activator.CreateInstance(target.Type);
            var param = benchmark.Parameters[0];
            target.Type.GetProperty(param.Definition.Name).SetMethod.Invoke(instance, new[] { param.Value });
            target.SetupMethod.Invoke(instance, new object[0]);
            var propName = target.MethodTitle.Replace("Serialization", "").Replace("Deserialization", "") + "ByteSize";
            return target.Type.GetProperty(propName).GetMethod.Invoke(instance, new object[0]).ToString();
        }

        public bool IsAvailable(Summary summary)
        {
            return true;
        }

        public string ColumnName => "Byte Size";
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Job;
    }

    [Config(typeof(Config))]
    public class EventSerializationBenchmark
    {
        class Config : ManualConfig
        {
            public Config()
            {
                Add(Parse(new[] { "diagnosers=Memory" }));
                UnionRule = ConfigUnionRule.AlwaysUseLocal;
                Add(DefaultConfig.Instance.GetJobs().ToArray());
                Add(DefaultConfig.Instance.GetAnalysers().ToArray());
                Add(DefaultConfig.Instance.GetExporters().ToArray());
                Add(DefaultConfig.Instance.GetDiagnosers().ToArray());
                Add(DefaultConfig.Instance.GetLoggers().ToArray());
                Add(DefaultConfig.Instance.GetValidators().ToArray());
                Set(DefaultConfig.Instance.GetOrderProvider());

                Add(new ColumnOrderFirst(new TagColumn("Implementation", name => name.Replace("Serialization", "").Replace("Deserialization", ""))));
                Add(new ColumnOrderFirst(new TagColumn("Direction", name => name.Contains("Serialization") ? "Serialization" : "Deserialization")));
                Add(new ByteSizeColumn());
                Add(StatisticColumn.Median);
                Add(StatisticColumn.StdDev);
                Add(StatisticColumn.OperationsPerSecond);
            }
        }

        IEventSerializer _eventSerializer;
        IEventDeserializer _eventDeserializer;
        ByteBufferWriter _writer;
        Event _ev;
        ByteBuffer _btdbSerializedData;
        MemoryStream _memStream;
        RuntimeTypeModel Serializer;
        Type _eventType;

        [Params("Simple", "Complex")]
        public string Complexity { get; set; }

        [Setup]
        public void Setup()
        {
            if (Complexity == "Simple")
                _ev = TestData.SimpleEventInstance();
            else
                _ev = TestData.ComplexEventInstance();

            // BTDB Setup
            _eventSerializer = new EventSerializer();
            _eventDeserializer = new EventDeserializer();
            _writer = new ByteBufferWriter();
            _eventSerializer.Serialize(_writer, _ev);
            var meta = _writer.GetDataAndRewind().ToAsyncSafe();
            _eventSerializer.ProcessMetadataLog(meta);
            _eventDeserializer.ProcessMetadataLog(meta);
            _eventSerializer.Serialize(_writer, _ev);
            _btdbSerializedData = _writer.GetDataAndRewind().ToAsyncSafe();
            BtdbByteSize = _btdbSerializedData.Length;
            _eventDeserializer.Deserialize(_btdbSerializedData).ShouldBeEquivalentTo(_ev);

            // ProtoBuf Setup
            Serializer = ModelFactory.CreateModel();
            _eventType = typeof(Event);
            _memStream = new MemoryStream();
            Serializer.Serialize(_memStream, _ev);
            ProtoBufByteSize = (int)_memStream.Length;
            _memStream.Position = 0;
            Serializer.Deserialize(_memStream, null, _eventType).ShouldBeEquivalentTo(_ev);

            BtdbSerialization();
            BtdbDeserialization();
            ProtoBufSerialization();
            ProtoBufDeserialization();
        }

        public int BtdbByteSize { get; set; }
        public int ProtoBufByteSize { get; set; }

        [Benchmark]
        public void BtdbSerialization()
        {
            _eventSerializer.Serialize(_writer, _ev);
            _writer.GetDataAndRewind();
        }

        [Benchmark]
        public void BtdbDeserialization()
        {
            _eventDeserializer.Deserialize(_btdbSerializedData);
        }

        [Benchmark]
        public void ProtoBufSerialization()
        {
            _memStream.Position = 0;
            Serializer.Serialize(_memStream, _ev);
        }

        [Benchmark]
        public void ProtoBufDeserialization()
        {
            _memStream.Position = 0;
            Serializer.Deserialize(_memStream, null, _eventType);
        }
    }
}
