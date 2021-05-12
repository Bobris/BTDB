using System;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.ConsoleArguments;
using BenchmarkDotNet.Loggers;
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

        public string GetValue(Summary summary, BenchmarkCase benchmark)
        {
            return _parent.GetValue(summary, benchmark);
        }

        public bool IsAvailable(Summary summary)
        {
            return _parent.IsAvailable(summary);
        }

        public bool IsDefault(Summary summary, BenchmarkCase benchmark)
        {
            return _parent.IsDefault(summary, benchmark);
        }

        public string GetValue(Summary summary, BenchmarkCase benchmark, SummaryStyle style)
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
        public string GetValue(Summary summary, BenchmarkCase benchmark)
        {
            var target = benchmark.Descriptor.Type;
            var instance = Activator.CreateInstance(benchmark.Descriptor.Type);
            var param = benchmark.Parameters[0];
            target.GetProperty(param.Definition.Name)!.SetMethod!.Invoke(instance, new[] { param.Value });
            var propName = benchmark.DisplayInfo.Replace("Serialization", "").Replace("Deserialization", "") + "ByteSize";
            return target.GetProperty(propName)!.GetMethod!.Invoke(instance, new object[0])!.ToString()!;
        }

        public bool IsAvailable(Summary summary)
        {
            return true;
        }

        public bool IsDefault(Summary summary, BenchmarkCase benchmark)
        {
            return true;
        }

        public string GetValue(Summary summary, BenchmarkCase benchmark, SummaryStyle style)
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
                Add(ConfigParser.Parse(new[] { "diagnosers=Memory" }, new ConsoleLogger()).config);
                UnionRule = ConfigUnionRule.AlwaysUseLocal;
                AddJob(DefaultConfig.Instance.GetJobs().ToArray());
                AddAnalyser(DefaultConfig.Instance.GetAnalysers().ToArray());
                AddExporter(DefaultConfig.Instance.GetExporters().ToArray());
                AddDiagnoser(DefaultConfig.Instance.GetDiagnosers().ToArray());
                AddLogger(DefaultConfig.Instance.GetLoggers().ToArray());
                AddValidator(DefaultConfig.Instance.GetValidators().ToArray());

                AddColumn(new ColumnOrderFirst(new TagColumn("Implementation", name => name.Replace("Serialization", "").Replace("Deserialization", ""))));
                AddColumn(new ColumnOrderFirst(new TagColumn("Direction", name => name.Contains("Serialization") ? "Serialization" : "Deserialization")));
                AddColumn(new ByteSizeColumn());
                AddColumn(StatisticColumn.Median);
                AddColumn(StatisticColumn.StdDev);
                AddColumn(StatisticColumn.OperationsPerSecond);
            }
        }

        IEventSerializer? _eventSerializer;
        IEventDeserializer? _eventDeserializer;
        Event? _ev;
        ByteBuffer _btdbSerializedData;
        MemoryStream? _memStream;
        RuntimeTypeModel? _serializer;
        Type? _eventType;

        [Params("Simple", "Complex")]
        public string? Complexity { get; set; }

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
            var meta = _eventSerializer.Serialize(out _, _ev).ToAsyncSafe();
            _eventSerializer.ProcessMetadataLog(meta);
            _eventDeserializer.ProcessMetadataLog(meta);
            _btdbSerializedData = _eventSerializer.Serialize(out _, _ev).ToAsyncSafe();
            BtdbByteSize = _btdbSerializedData.Length;
            _eventDeserializer.Deserialize(out object obj, _btdbSerializedData);
            obj.Should().BeEquivalentTo(_ev);

            // ProtoBuf Setup
            _serializer = ModelFactory.CreateModel();
            _eventType = typeof(Event);
            _memStream = new MemoryStream();
            _serializer.Serialize(_memStream, _ev);
            ProtoBufByteSize = (int)_memStream.Length;
            _memStream.Position = 0;
            _serializer.Deserialize(_memStream, null, _eventType).Should().BeEquivalentTo(_ev);

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
            _eventSerializer!.Serialize(out _, _ev!);
        }

        [Benchmark]
        public void BtdbDeserialization()
        {
            _eventDeserializer!.Deserialize(out object _, _btdbSerializedData);
        }

        [Benchmark]
        public void ProtoBufSerialization()
        {
            _memStream!.Position = 0;
            _serializer!.Serialize(_memStream, _ev);
        }

        [Benchmark]
        public void ProtoBufDeserialization()
        {
            _memStream!.Position = 0;
            _serializer!.Deserialize(_memStream, null, _eventType);
        }
    }
}
