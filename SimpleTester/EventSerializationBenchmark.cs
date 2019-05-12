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

        public bool IsDefault(Summary summary, Benchmark benchmark)
        {
            return _parent.IsDefault(summary, benchmark);
        }

        public string GetValue(Summary summary, Benchmark benchmark, ISummaryStyle style)
        {
            return _parent.GetValue(summary, benchmark, style);
        }

        public string ColumnName => _parent.ColumnName;
        public bool AlwaysShow => _parent.AlwaysShow;
        public ColumnCategory Category => ColumnCategory.Job;
        
        public string Id => _parent.Id;

        public int PriorityInCategory => _parent.PriorityInCategory;

        public bool IsNumeric => _parent.IsNumeric;

        public UnitType UnitType => _parent.UnitType;

        public string Legend => _parent.Legend;
    }

    public class ByteSizeColumn : IColumn
    {
        public string GetValue(Summary summary, Benchmark benchmark)
        {
            var target = benchmark.Target;
            var instance = Activator.CreateInstance(target.Type);
            var param = benchmark.Parameters[0];
            target.Type.GetProperty(param.Definition.Name).SetMethod.Invoke(instance, new[] { param.Value });
            target.IterationSetupMethod.Invoke(instance, new object[0]);
            var propName = target.MethodDisplayInfo.Replace("Serialization", "").Replace("Deserialization", "") + "ByteSize";
            return target.Type.GetProperty(propName).GetMethod.Invoke(instance, new object[0]).ToString();
        }

        public bool IsAvailable(Summary summary)
        {
            return true;
        }

        public bool IsDefault(Summary summary, Benchmark benchmark)
        {
            return true;
        }

        public string GetValue(Summary summary, Benchmark benchmark, ISummaryStyle style)
        {
            return GetValue(summary, benchmark);
        }

        public string ColumnName => "Byte Size";
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Job;

        public string Id => "ByteSize";

        public int PriorityInCategory => 0;

        public bool IsNumeric => true;

        public UnitType UnitType => UnitType.Size;

        public string Legend => "";
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
        Event _ev;
        ByteBuffer _btdbSerializedData;
        MemoryStream _memStream;
        RuntimeTypeModel Serializer;
        Type _eventType;

        [Params("Simple", "Complex")]
        public string Complexity { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            if (Complexity == "Simple")
                _ev = TestData.SimpleEventInstance();
            else
                _ev = TestData.ComplexEventInstance();

            // BTDB Setup
            _eventSerializer = new EventSerializer();
            _eventDeserializer = new EventDeserializer();
            bool hasMedataData;
            var meta = _eventSerializer.Serialize(out hasMedataData, _ev).ToAsyncSafe();
            _eventSerializer.ProcessMetadataLog(meta);
            _eventDeserializer.ProcessMetadataLog(meta);
            _btdbSerializedData = _eventSerializer.Serialize(out hasMedataData, _ev).ToAsyncSafe();
            BtdbByteSize = _btdbSerializedData.Length;
            object obj;
            _eventDeserializer.Deserialize(out obj, _btdbSerializedData);
            obj.Should().BeEquivalentTo(_ev);

            // ProtoBuf Setup
            Serializer = ModelFactory.CreateModel();
            _eventType = typeof(Event);
            _memStream = new MemoryStream();
            Serializer.Serialize(_memStream, _ev);
            ProtoBufByteSize = (int)_memStream.Length;
            _memStream.Position = 0;
            Serializer.Deserialize(_memStream, null, _eventType).Should().BeEquivalentTo(_ev);

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
            bool hasMetaData;
            _eventSerializer.Serialize(out hasMetaData, _ev);
        }

        [Benchmark]
        public void BtdbDeserialization()
        {
            object obj;
            _eventDeserializer.Deserialize(out obj, _btdbSerializedData);
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
