using System;
using System.Diagnostics;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace ODbDump.Visitor
{
    class ToConsoleSizeVisitor : IODBVisitor
    {
        long _currentMemorySize;
        long _currentOnDiskSize;
        string _currentRelation;
        string _currentSingleton;
        bool _headerWritten;
        readonly Stopwatch _stopWatch = new Stopwatch();

        const int KeyOverhead = 20;

        public void MarkCurrentKeyAsUsed(IKeyValueDBTransaction tr)
        {
            var (memory, disk) = tr.GetStorageSizeOfCurrentKey();
            _currentMemorySize += memory + KeyOverhead;
            _currentOnDiskSize += disk;
        }

        void Flush()
        {
            if (!_headerWritten)
            {
                Console.WriteLine("name,memory,disk,type,iteration(s)");
                _headerWritten = true;
            }
            else
            {
                _stopWatch.Stop();
            }

            var elapsed = _stopWatch.Elapsed.TotalSeconds;
            if (!string.IsNullOrEmpty(_currentRelation))
            {
                Console.WriteLine($"{_currentRelation},{_currentMemorySize},{_currentOnDiskSize},relation,{elapsed:F2}");
                _currentRelation = null;
            } else if (!string.IsNullOrEmpty(_currentSingleton))
            {
                Console.WriteLine($"{_currentSingleton},{_currentMemorySize},{_currentOnDiskSize},singleton,{elapsed:F2}");
                _currentSingleton = null;
            }
            _currentMemorySize = 0;
            _currentOnDiskSize = 0;
            _stopWatch.Restart();
        }

        public bool VisitSingleton(uint tableId, string tableName, ulong oid)
        {
            Flush();
            _currentSingleton = tableName;
            return true;
        }

        public bool StartObject(ulong oid, uint tableId, string tableName, uint version)
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

        public void ScalarAsObject(object content)
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

        public bool StartInlineObject(uint tableId, string tableName, uint version)
        {
            return true;
        }

        public void EndInlineObject()
        {
        }

        public bool StartList()
        {
            return true;
        }

        public bool StartItem()
        {
            return true;
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
            return false;
        }

        public void EndDictKey()
        {
        }

        public bool StartDictValue()
        {
            return true;
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
            return true;
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

        public bool StartRelation(string relationName)
        {
            Flush();
            _currentRelation = relationName;
            return true;
        }

        public bool StartRelationKey()
        {
            return false;
        }

        public void EndRelationKey()
        {
        }

        public bool StartRelationValue()
        {
            return true;
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
