using System;
using System.IO;
using System.Text;

namespace SimpleTester
{
    public class ClassGenerator
    {
        StringBuilder? _sb;
        StringBuilder? _sbInst;
        Random? _rand;

        public void Run()
        {
            _sb = new StringBuilder();
            _sbInst = new StringBuilder();
            _rand = new Random(1);
            _sb.Append(@"using System;
using System.Collections.Generic;

namespace SimpleTester
{
");
            for (var i = 0; i < 1000; i++)
            {
                GenerateClass(i);
            }
            _sb.AppendLine(@"
    public class InstantiateALotOfClasses {
        public static IEnumerable<object> Gen() {");
            _sb.Append(_sbInst);
            _sb.AppendLine(@"       }
    }
}");
            File.WriteAllText("ALotOfClasses.cs", _sb.ToString());
        }

        void GenerateClass(int i)
        {
            _sb!.Append($@"
    public class TestKlass{i} 
    {{");
            _sbInst!.Append($@"
        yield return new TestKlass{i} {{");
            var fieldsCount = _rand!.Next(2, 5);
            for (var j = 0; j < fieldsCount; j++)
            {
                switch (_rand.Next(4))
                {
                    case 0:
                        if (i > 0)
                        {
                            var k = _rand.Next(i);
                            _sb.Append($@"
        public TestKlass{k} Prop{i}_{j} {{ get; set; }}");
                            _sbInst.Append($@"
            Prop{i}_{j} = new TestKlass{k}(),");
                        }
                        break;
                    case 1:
                        _sb.Append($@"
        public List<string> Prop{i}_{j} {{ get; set; }}");
                        _sbInst.Append($@"
            Prop{i}_{j} = new List<string> {{ ""A"" }},");
                        break;
                    case 2:
                        _sb.Append($@"
        public class LocalClass{j} {{
            public LocalClass{j} Self {{ get; set; }}
        }}
        public Dictionary<int, LocalClass{j}> Prop{i}_{j} {{ get; set; }}");
                        _sbInst.Append($@"
            Prop{i}_{j} = new Dictionary<int, TestKlass{i}.LocalClass{j}> {{ {{ 1, new TestKlass{i}.LocalClass{j}() }} }},");
                        break;
                    case 3:
                        _sb.Append($@"
        public class LocalClass{j} {{
            public LocalClass{j} Self {{ get; set; }}
        }}
        public Dictionary<int, List<LocalClass{j}>> Prop{i}_{j} {{ get; set; }}");
                        _sbInst.Append($@"
            Prop{i}_{j} = new Dictionary<int, List<TestKlass{i}.LocalClass{j}>> {{ {{ 1, new List<TestKlass{i}.LocalClass{j}> {{ new TestKlass{i}.LocalClass{j}() }} }} }},");
                        break;
                }
            }
            _sb.Append(@"
    }
");
            _sbInst.Append($@"
        }};
");
        }
    }
}
