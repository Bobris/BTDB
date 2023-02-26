using System;
using System.Collections.Generic;
using System.IO;
using BTDB.KVDBLayer;

namespace ODbDump.TrlDump
{
    public class TrlVisitor : ITrlVisitor
    {
        TextWriter _out;
        readonly Dictionary<ulong, ulong> _relationSizes = new(); //relationId -> total bytes written
        readonly Dictionary<ulong, ulong> _dictSizes = new(); //dictId -> total bytes written
        readonly Dictionary<ulong, ulong> _objSizes = new(); //objId -> total bytes written

        public TrlVisitor(TextWriter? @out = null)
        {
            _out = @out ?? Console.Out;
        }

        public void StartFile(uint index, ulong size)
        {
            _out.WriteLine($"StartFile idx: {index} size: {size}");
        }

        public void StartOperation(KVCommandType type)
        {
            _out.Write(type.ToString());
        }

        public void OperationDetail(string detail)
        {
            _out.Write(" " + detail);
        }

        public void UpsertObject(ulong oid, uint tableId, int keyLength, int valueLength)
        {
            OperationDetail(
                $"{(valueLength > 500000 ? "bigobject" : "object")} oid: {oid} tableId: {tableId} keyLength: {keyLength} valueLength: {valueLength}");
            AddSize(_objSizes, oid, keyLength + valueLength);
        }

        public void UpsertODBDictionary(ulong oid, int keyLength, int valueLength)
        {
            OperationDetail($"IDictionary value dicid: {oid} keyLength: {keyLength} valueLength: {valueLength}");
            AddSize(_dictSizes, oid, keyLength + valueLength);
        }

        public void UpsertRelationValue(ulong relationIdx, int keyLength, int valueLength)
        {
            OperationDetail($"Relation id {relationIdx} keyLength: {keyLength} valueLength: {valueLength}");
            AddSize(_relationSizes, relationIdx, keyLength + valueLength);
        }

        public void UpsertRelationSecondaryKey(ulong relationIdx, int skIndex, int keyLength, int valueLength)
        {
            OperationDetail(
                $"Secondary key relIdx {relationIdx} skIdx {skIndex} keyLength: {keyLength} valueLength: {valueLength}");
            AddSize(_relationSizes, relationIdx, keyLength + valueLength);
        }

        public void EraseObject(ulong oid)
        {
            OperationDetail($"oid: {oid}");
        }

        public void EraseODBDictionary(ulong oid, int keyLength)
        {
            OperationDetail($"IDictionary value dicid: {oid} keyLength: {keyLength}");
        }

        public void EraseRelationValue(ulong relationIdx, int keyLength)
        {
            OperationDetail($"Relation id {relationIdx} keyLength: {keyLength}");
        }

        public void EndOperation()
        {
            _out.WriteLine("");
        }

        public void WriteStatistics()
        {
            int limit = 100000; //do not report lower volumes
            WriteInfoAboutSizes("Dictionaries statistics", _dictSizes, limit);
            WriteInfoAboutSizes("Relations statistics", _relationSizes, limit);
            WriteInfoAboutSizes("Objects statistics", _objSizes, limit);
        }

        void AddSize(Dictionary<ulong, ulong> dict, ulong key, int add)
        {
            if (!dict.TryAdd(key, (ulong) add))
            {
                dict[key] += (ulong) add;
            }
        }

        void WriteInfoAboutSizes(string name, Dictionary<ulong, ulong> sizes, int limit)
        {
            _out.WriteLine($"STAT: {name}");
            foreach (var (k, v) in sizes)
            {
                if (v < (ulong) limit) continue;
                _out.WriteLine($"{k}: {v}");
            }
        }
    }
}
