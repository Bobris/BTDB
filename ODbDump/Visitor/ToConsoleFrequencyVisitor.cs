using System;
using System.Collections.Generic;
using BTDB.ODBLayer;

namespace ODbDump.Visitor
{
    class ToConsoleFrequencyVisitor : ToConsoleFastVisitor, IODBVisitor
    {
        readonly Dictionary<string, int> _relationFrequency = new Dictionary<string, int>();
        readonly Dictionary<string, int> _singletonFrequency = new Dictionary<string, int>();

        string? _currentRelation;
        string? _currentSingleton;
        int _currentCount;

        public void OutputStatistic()
        {
            Console.WriteLine("name, count, type");
            foreach (var kv in _relationFrequency)
            {
                Console.WriteLine($"{kv.Key},{kv.Value},relation");
            }

            foreach (var kv in _singletonFrequency)
            {
                Console.WriteLine($"{kv.Key},{kv.Value},singleton");
            }
        }

        void Flush()
        {
            if (!string.IsNullOrEmpty(_currentRelation))
            {
                _relationFrequency.Add(_currentRelation!, _currentCount);
                _currentRelation = null;
            }
            else if (!string.IsNullOrEmpty(_currentSingleton))
            {
                _singletonFrequency.Add(_currentSingleton!, _currentCount);
                _currentSingleton = null;
            }

            _currentCount = 0;
        }


        public bool VisitSingleton(uint tableId, string? tableName, ulong oid)
        {
            Flush();
            _currentSingleton = tableName;
            return true;
        }


        public bool StartObject(ulong oid, uint tableId, string? tableName, uint version)
        {
            return true;
        }

        public bool StartField(string name)
        {
            return true;
        }

        public bool NeedScalarAsObject()
        {
            return false;
        }

        public void ScalarAsObject(object? content)
        {
        }

        public bool NeedScalarAsText()
        {
            return false;
        }

        public void ScalarAsText(string content)
        {
        }

        public void OidReference(ulong oid)
        {
        }

        public bool StartInlineObject(uint tableId, string? tableName, uint version)
        {
            return false;
        }

        public void EndInlineObject()
        {
        }

        public bool StartList()
        {
            return false;
        }

        public bool StartItem()
        {
            return false;
        }

        public void EndItem()
        {
        }

        public void EndList()
        {
        }

        public bool StartDictionary()
        {
            return true;
        }

        public bool StartDictKey()
        {
            _currentCount++;
            return false;
        }

        public void EndDictKey()
        {
        }

        public bool StartDictValue()
        {
            return false;
        }

        public void EndDictValue()
        {
        }

        public void EndDictionary()
        {
        }

        public bool StartSet()
        {
            return true;
        }

        public bool StartSetKey()
        {
            _currentCount++;
            return false;
        }

        public void EndSetKey()
        {
        }

        public void EndSet()
        {
        }

        public void EndField()
        {
        }

        public void EndObject()
        {
        }

        public bool StartRelation(ODBIteratorRelationInfo relationInfo)
        {
            Flush();
            _currentRelation = relationInfo.Name;
            return true;
        }

        public bool StartRelationKey()
        {
            _currentCount++;
            return false;
        }

        public void EndRelationKey()
        {
        }

        public bool StartRelationValue()
        {
            return false;
        }

        public void EndRelationValue()
        {
        }

        public void EndRelation()
        {
            Flush();
        }

        public void InlineBackRef(int iid)
        {
        }

        public void InlineRef(int iid)
        {
        }
    }
}
